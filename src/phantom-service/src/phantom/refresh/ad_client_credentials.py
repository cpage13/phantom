"""AdMinter - autonomous AD client-credentials token mint (ADR-001).

Uses ``azure.identity.aio.ClientSecretCredential`` to mint a token
under Phantom's own AD app registration. A background loop mints
proactively ``refresh_seconds_before_expiry`` before expiry; ``on_401``
schedules an immediate mint.

The endpoint+uid the minter writes to is determined per-instance from
the :class:`phantom.config.ad_mint.AdMintConfig` block on the instance's
:class:`InstanceCfg`. The driving use is one ``(endpoint, uid)``
per instance.

The configured ``endpoint`` is normalised through
:func:`phantom.routing.host_key_for` before it becomes a cache key, so the
minter writes the SAME key space the reader looks up (SW-2). It previously
wrote the YAML value verbatim: an operator spelling the natural
``https://files.upstream.example`` minted successfully into a key nothing
ever read, every row parked in ``auth_expired``, the kicker (which probes
the normalised host) woke none of them, and nothing logged an error.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
from datetime import UTC, datetime
from enum import StrEnum

from phantom.config.ad_mint import AdMintConfig
from phantom.routing import host_key_for
from phantom.storage.interface import TokenCache

logger = logging.getLogger(__name__)

# Hard floor on the wait between two mints, applied AFTER the jitter
# subtraction. Without it the arithmetic could produce a non-positive wait
# (``asyncio.wait_for`` with a negative timeout raises TimeoutError with zero
# delay), and the loop would mint against the authority's token endpoint as
# fast as the event loop allows until Entra ID throttled the app registration
# or locked it. Thirty seconds is well below any healthy refresh interval, so
# it never shapes a correct schedule, and it bounds the pathological case to
# two mints a minute - a rate no authority treats as abuse.
_MIN_MINT_WAIT_SECONDS: float = 30.0

# The last-resort positive wait, used when even half the remaining lifetime is
# tiny. Its only job is to keep the wait strictly positive so
# ``asyncio.wait_for`` actually waits; the proportionate floor above does the
# rate limiting.
_ABSOLUTE_MIN_WAIT: float = 1.0


class AuthUnavailableError(Exception):
    """Raised when neither primary nor secondary AD credentials succeed."""


class AdReachability(StrEnum):
    """What this minter has actually observed at the AD token endpoint.

    The producer behind ``GET /v1/admin/status``'s ``ad_reachability``
    (S1-7, ADR-007). Three states, and only one of them is a claim about
    the authority answering:

    * :attr:`NOT_ATTEMPTED` - no mint cycle has completed yet, so nothing
      has been observed. The minter mints on its first loop iteration, so
      this holds for one cycle at boot.
    * :attr:`REACHABLE` - the most recent cycle obtained a token.
    * :attr:`UNREACHABLE` - the most recent cycle exhausted every
      configured credential without obtaining a token.
    """

    NOT_ATTEMPTED = "not_attempted"
    REACHABLE = "reachable"
    UNREACHABLE = "unreachable"


class AdMinter:
    """ADR-001 ``ad_client_credentials`` autonomous-mint engine.

    The composition root constructs one ``AdMinter`` per instance that
    sets :attr:`phantom.config.settings.InstanceCfg.ad_mint`; instances
    without an ``ad_mint`` block carry ``minter=None`` on their
    :class:`InstanceContext`.
    """

    def __init__(self, *, config: AdMintConfig, token_cache: TokenCache) -> None:
        """Construct the minter.

        Args:
            config: Typed AD-mint configuration. Sourced from the
                instance's :attr:`InstanceCfg.ad_mint` block.
            token_cache: The instance's token cache; minted tokens land
                here via :meth:`TokenCache.set`.
        """
        self._config = config
        self._cache = token_cache
        self._stop_event = asyncio.Event()
        self._immediate_mint = asyncio.Event()
        # SW-2: the cache key, normalised ONCE at construction through the
        # one hostname normaliser the reader (BearerAuthProvider) and the
        # kicker's wake probe already use, so the mint key equals the
        # lookup key by construction. ``config.endpoint`` stays the raw
        # operator spelling and is what the logs name.
        self._endpoint_key = host_key_for(config.endpoint)
        self._reachability = AdReachability.NOT_ATTEMPTED

    async def run(self, stop_event: asyncio.Event) -> None:
        """Drive the background mint loop until ``stop_event`` fires.

        H6 audit closure (Phase 2 § 3.2.5): the minter is no longer
        spawned via ``asyncio.create_task`` inside its own ``start()``
        method (which left the task unsupervised - a silent exception
        in the refresh loop would have looked identical to a healthy
        minter). The composition root - ``app.py``'s ``lifespan`` -
        now invokes ``minter.run()`` on its supervising
        ``asyncio.TaskGroup``; an unhandled exception propagates out as
        an ``ExceptionGroup`` and crashes the process visibly.

        Args:
            stop_event: External stop signal. The mint loop exits when
                this event is set OR when the supervising TaskGroup
                cancels this coroutine.
        """
        # Mirror ``stop_event`` into the internal one so ``on_401``
        # consumers can keep using ``_immediate_mint`` semantics
        # without knowing about the supervising stop event.
        self._stop_event = stop_event
        await self._refresh_loop()

    async def on_401(
        self,
        endpoint: str,
        uid: str,
        observed_at: datetime,
    ) -> None:
        """Schedule an immediate mint.

        The sender invokes this when a Phantom-injected cached token
        returned 401/403. The minter's loop wakes via ``_immediate_mint``
        and mints a fresh token into the cache.
        """
        del endpoint, uid, observed_at
        self._immediate_mint.set()

    async def _refresh_loop(self) -> None:
        """Background mint loop."""
        backoff = list(self._config.ad_outage_retry_seconds)
        outage_index = 0
        while not self._stop_event.is_set():
            try:
                expires_at = await self._mint_and_store()
                self._reachability = AdReachability.REACHABLE
                outage_index = 0
                # Sleep until refresh-before-expiry minus jitter, or wake on 401.
                wait = self._next_mint_wait_seconds(expires_at)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._immediate_mint.wait(),
                        timeout=wait,
                    )
                self._immediate_mint.clear()
            except AuthUnavailableError as exc:
                # Record the observation before the fail-fast re-raise, so
                # the last thing the status surface saw is the truth even
                # when this loop is about to die.
                self._reachability = AdReachability.UNREACHABLE
                if not backoff:
                    # Empty schedule means fail-fast: re-raise so the
                    # supervising TaskGroup observes the failure.
                    raise
                delay = backoff[min(outage_index, len(backoff) - 1)]
                outage_index += 1
                logger.warning("AD mint failed (%s); retry in %ds", exc, delay)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

    def _next_mint_wait_seconds(self, expires_at: datetime) -> float:
        """Seconds to wait before minting a replacement for a token.

        The schedule is "the token's remaining lifetime, less the pre-expiry
        refresh margin, less a jitter draw", floored so it is always strictly
        positive. The floor is applied LAST, after the jitter subtraction:
        applied before it (as it once was) the subtraction could take the
        floored value negative, and ``asyncio.wait_for`` treats a negative
        timeout as already-expired, so the loop minted continuously.

        Args:
            expires_at: Expiry of the token just minted, from the authority's
                own ``expires_on``.

        Returns:
            The wait in seconds, always strictly positive and never later
            than the token's own expiry.
        """
        refresh_before = self._config.refresh_seconds_before_expiry
        jitter = self._config.refresh_jitter_seconds
        lifetime_remaining = (expires_at - datetime.now(tz=UTC)).total_seconds()
        scheduled = lifetime_remaining - refresh_before - random.uniform(0.0, jitter)
        # The floor is PROPORTIONATE to the token's own lifetime, not a flat
        # constant. A flat 30 s floor was wrong in a way the original comment
        # asserted it was not ("well below any healthy refresh interval, so it
        # never shapes a correct schedule"): a legitimately short-lived token,
        # which the AD-mint e2e uses and which real app registrations can be
        # configured for, has a whole lifetime under a minute, so a flat floor
        # overrides its correct schedule and the token expires before the
        # replacement is minted.
        #
        # Half the remaining lifetime is never sooner than the token needs and
        # never later than its expiry, and the absolute guard still caps the
        # rate for a long-lived token whose margin was misconfigured. Both
        # goals hold: the wait is always strictly positive, so ``wait_for``
        # cannot treat it as already-expired and spin, and the mint rate stays
        # bounded by the lifetime rather than by an unrelated constant.
        floor = min(_MIN_MINT_WAIT_SECONDS, max(lifetime_remaining / 2.0, _ABSOLUTE_MIN_WAIT))
        if scheduled < floor:
            # Not a hypothetical: pinning refresh_seconds_before_expiry above
            # the token's actual lifetime lands here on EVERY cycle, so say so
            # rather than quietly minting at the floor rate forever.
            logger.warning(
                "AD mint refresh window collapsed for endpoint=%s uid=%s: token "
                "lifetime %.1fs vs refresh_seconds_before_expiry=%d (+ up to "
                "%.1fs jitter) computes a %.1fs wait; using the %.1fs floor. "
                "Lower refresh_seconds_before_expiry or check the app "
                "registration's token lifetime",
                self._config.endpoint,
                self._config.uid,
                lifetime_remaining,
                refresh_before,
                jitter,
                scheduled,
                floor,
            )
        return max(floor, scheduled)

    async def _mint_and_store(self) -> datetime:
        """Mint a token via azure-identity and write it to the cache.

        Returns:
            The expiry datetime of the freshly minted token.

        Raises:
            AuthUnavailableError: When both primary and secondary mints fail.
        """
        primary_env = self._config.primary_client_secret_env
        secondary_env = self._config.secondary_client_secret_env
        scope = self._config.scope
        primary_secret = os.environ.get(primary_env)
        if primary_secret:
            try:
                expiry = await self._mint(primary_secret, scope)
                return expiry
            except Exception as exc:
                logger.warning("Primary AD mint failed: %s", exc)
        if secondary_env:
            secondary_secret = os.environ.get(secondary_env)
            if secondary_secret:
                try:
                    return await self._mint(secondary_secret, scope)
                except Exception as exc:
                    logger.warning("Secondary AD mint failed: %s", exc)
        raise AuthUnavailableError("No AD credentials produced a token")

    async def _mint(self, client_secret: str, scope: str) -> datetime:
        """Mint one token using azure-identity and write it to the cache.

        The cache write uses :attr:`_endpoint_key`, the normalised form of
        the configured endpoint, NOT the raw YAML string (SW-2).

        Args:
            client_secret: The client secret to authenticate the app
                registration with (primary or secondary).
            scope: The AD scope to request the token for.

        Returns:
            The expiry datetime of the freshly minted token.
        """
        # Lazy import - azure-identity is heavy and instances without an
        # AdMinter never need to import it.
        from azure.identity.aio import ClientSecretCredential

        cred = ClientSecretCredential(
            tenant_id=self._config.tenant_id,
            client_id=self._config.client_id,
            client_secret=client_secret,
            authority=self._config.authority_url,
        )
        try:
            access = await cred.get_token(scope)
        finally:
            await cred.close()
        expiry = datetime.fromtimestamp(access.expires_on, tz=UTC)
        await self._cache.set(
            endpoint=self._endpoint_key,
            uid=self._config.uid,
            bearer=f"Bearer {access.token}",
            source="plugin_mint",
        )
        return expiry

    @property
    def reachability(self) -> AdReachability:
        """What this minter last observed at the AD token endpoint.

        Read by ``GET /v1/admin/status`` to fill ``ad_reachability``
        (S1-7). Before this existed the field was a hardcoded literal with
        no producer anywhere, so the one signal designed to tell an
        operator "your app registration is unreachable and that is why
        every row is parking in auth_expired" never fired.

        Returns:
            The observed reachability; :attr:`AdReachability.NOT_ATTEMPTED`
            until the first mint cycle completes.
        """
        return self._reachability
