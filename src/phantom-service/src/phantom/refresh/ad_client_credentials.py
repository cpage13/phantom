"""AdMinter — autonomous AD client-credentials token mint (ADR-001).

Uses ``azure.identity.aio.ClientSecretCredential`` to mint a token
under Phantom's own AD app registration. A background loop mints
proactively ``refresh_seconds_before_expiry`` before expiry; ``on_401``
schedules an immediate mint.

The endpoint+uid the minter writes to is determined per-instance from
the :class:`phantom.config.ad_mint.AdMintConfig` block on the instance's
:class:`InstanceCfg`. The driving use is one ``(endpoint, uid)``
per instance.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
from datetime import UTC, datetime

from phantom.config.ad_mint import AdMintConfig
from phantom.storage.interface import TokenCache

logger = logging.getLogger(__name__)


class AuthUnavailableError(Exception):
    """Raised when neither primary nor secondary AD credentials succeed."""


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

    async def run(self, stop_event: asyncio.Event) -> None:
        """Drive the background mint loop until ``stop_event`` fires.

        H6 audit closure (Phase 2 § 3.2.5): the minter is no longer
        spawned via ``asyncio.create_task`` inside its own ``start()``
        method (which left the task unsupervised — a silent exception
        in the refresh loop would have looked identical to a healthy
        minter). The composition root — ``app.py``'s ``lifespan`` —
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
                outage_index = 0
                # Sleep until refresh-before-expiry minus jitter, or wake on 401.
                refresh_before = self._config.refresh_seconds_before_expiry
                jitter = self._config.refresh_jitter_seconds
                wait = max(
                    1.0,
                    (expires_at - datetime.now(tz=UTC)).total_seconds() - refresh_before,
                )
                wait -= random.uniform(0, jitter)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._immediate_mint.wait(),
                        timeout=wait,
                    )
                self._immediate_mint.clear()
            except AuthUnavailableError as exc:
                if not backoff:
                    # Empty schedule means fail-fast: re-raise so the
                    # supervising TaskGroup observes the failure.
                    raise
                delay = backoff[min(outage_index, len(backoff) - 1)]
                outage_index += 1
                logger.warning("AD mint failed (%s); retry in %ds", exc, delay)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

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
        """Mint one token using azure-identity and write it to the cache."""
        # Lazy import — azure-identity is heavy and instances without an
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
            endpoint=self._config.endpoint,
            uid=self._config.uid,
            bearer=f"Bearer {access.token}",
            source="plugin_mint",
        )
        return expiry

    def trigger_immediate_mint_for_test(self) -> None:
        """Test hook — set the immediate-mint event."""
        self._immediate_mint.set()

    @property
    def latest_expiry(self) -> datetime | None:
        """Best-effort: most recent mint's expiry (None if never minted).

        Not tracked separately; admin status derives it from the cache.
        """
        return None
