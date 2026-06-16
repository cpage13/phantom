"""Chain executor — the load-bearing primitive.

One call to :meth:`ChainExecutor.execute_one_step` runs **one step**
of the chain identified by ``row.current_step_index``. The executor
owns:

1. The capture-TTL gate (ADR-011) — checked first.
2. ``{{step.var}}`` placeholder substitution in URL, headers, and JSON body.
3. Auth injection — looks up ``token_cache.get(endpoint, uid)`` when the
   route is ``phantom_bearer``.
4. Idempotency-header injection per the step's ``idempotency_header``.
5. The HTTP send through :class:`UpstreamClient`.
6. Capture extraction from the response (JSONPath, first-match).
7. Result classification (Succeeded / 4xx / 5xx / FailedAuth /
   FailedNetwork / TemplateUnresolved / CaptureExpiredStored).
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import httpx

from phantom.chain.jsonpath import extract, find_placeholders, substitute
from phantom.chain.parser import envelope_from_persistence_json
from phantom.config.settings import InstanceCfg
from phantom.models.chain import (
    ChainBodyBytes,
    ChainBodyJson,
    ChainBodyRef,
    ChainBodyText,
    ChainEnvelope,
    ChainStep,
)
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.routing import ResolvedRoute
from phantom.storage.interface import TokenCache
from phantom.transport.interface import UpstreamClient, UpstreamRequest

logger = logging.getLogger(__name__)

# Phantom's reserved header namespace. Producers use ``X-Phantom-*``
# headers at the producer→Phantom ingress boundary (X-Phantom-Uid,
# X-Phantom-Idempotency-Key, X-Phantom-Instance, X-Phantom-Group-Id,
# X-Phantom-Multifile-Id, X-Phantom-Order). These headers are
# Phantom's internal control channel and must NEVER be forwarded to
# upstream — that would violate the transparent-proxy invariant (producers
# that put X-Phantom-* into a chain step's headers would otherwise see
# them leak through).
_PHANTOM_RESERVED_HEADER_PREFIX = "x-phantom-"


@dataclass(frozen=True)
class Succeeded:
    """Step completed; ``captured`` is the new (or augmented) CapturedValues snapshot."""

    captured: CapturedValues
    next_step_index: int
    chain_done: bool
    step_name: str
    upstream_status: int
    upstream_headers: dict[str, str]


@dataclass(frozen=True)
class FailedAuth:
    """Phantom-injected auth was rejected (401/403)."""

    status: int
    observed_at: datetime


@dataclass(frozen=True)
class Failed4xx:
    """Non-auth 4xx — terminal."""

    status: int
    body: bytes


@dataclass(frozen=True)
class Failed5xx:
    """5xx — retry-eligible."""

    status: int


@dataclass(frozen=True)
class FailedNetwork:
    """Transport-class failure (timeout, connect refused)."""

    error: str


@dataclass(frozen=True)
class CaptureExpiredStored:
    """A referenced capture's TTL elapsed; row should transition to ``stored``."""

    producing_step: str


@dataclass(frozen=True)
class CaptureExpiredRewind:
    """ADR-011 reexecute=True — caller rewinds to ``rewind_to_step_index`` and re-queues."""

    producing_step: str
    rewind_to_step_index: int


@dataclass(frozen=True)
class TemplateUnresolved:
    """A placeholder did not resolve at execution time."""

    placeholder: str


@dataclass(frozen=True)
class CaptureIncomplete:
    """A 2xx response was MISSING a capture a later step requires (finding R7-5-B).

    The step returned a success status, but a capture this step DECLARES — and
    that a downstream step REFERENCES via ``{{step.capture}}`` — extracted to
    ``None`` (the response body was truncated / incomplete / a buggy proxy
    returned 2xx before the body was whole — the D-05/D-13/PAR "don't-trust"
    hazard). Advancing the step on the 2xx alone would strand the downstream
    step on an unresolvable template forever (the row wedges in ``attempting``
    holding a saturation slot). So this is classified as a RETRYABLE failure:
    the same step re-runs (and produces the captures once the upstream returns a
    complete body), bounded by the chain's max-attempts (then → ``stored``).
    """

    upstream_status: int
    missing_captures: tuple[str, ...]


ExecuteStepResult = (
    Succeeded
    | FailedAuth
    | Failed4xx
    | Failed5xx
    | FailedNetwork
    | CaptureExpiredStored
    | CaptureExpiredRewind
    | TemplateUnresolved
    | CaptureIncomplete
)


class ChainExecutor:
    """The primitive turning a :class:`ChainEnvelope` into upstream HTTP calls."""

    def __init__(
        self,
        *,
        token_cache: TokenCache,
        upstream_client: UpstreamClient,
        resolve_route: Callable[[str, InstanceCfg], ResolvedRoute],
        clock: Callable[[], datetime],
        instance: InstanceCfg,
    ) -> None:
        """Construct the executor.

        Args:
            token_cache: ADR-002 cache for ``(endpoint, uid)`` lookups.
            upstream_client: Transport seam.
            resolve_route: Per-instance route policy lookup. The
                composition root binds this to
                :func:`phantom.routing.resolve_route`.
            clock: A callable returning a UTC ``datetime`` — injectable
                for deterministic tests.
            instance: The instance whose routes apply.
        """
        self._cache = token_cache
        self._client = upstream_client
        self._resolve_route = resolve_route
        self._clock = clock
        self._instance = instance

    async def execute_one_step(
        self,
        row: UploadRow,
        body_refs: dict[str, bytes],
    ) -> ExecuteStepResult:
        """Run one step of ``row``'s chain and classify the result."""
        envelope = envelope_from_persistence_json(row.chain_envelope_json)
        step_index = row.current_step_index
        if step_index >= len(envelope.steps):
            raise ValueError(
                f"current_step_index {step_index} past end of chain (steps={len(envelope.steps)})",
            )
        step = envelope.steps[step_index]

        # (a) Capture-TTL gate (ADR-011).
        ttl_check = self._check_capture_ttl(step, row, envelope)
        if ttl_check is not None:
            return ttl_check

        # (b) Substitute placeholders in URL, headers, body.
        substituted_url, ok = substitute(step.url, self._captures_as_dict(row.captured_values))
        if not ok:
            return TemplateUnresolved(placeholder=step.url)
        substituted_headers: dict[str, str] = {}
        for name, value in step.headers.items():
            # Phantom's reserved header namespace (``X-Phantom-*``)
            # must not be forwarded to upstream — transparent-proxy
            # invariant. Strip case-insensitively.
            if name.lower().startswith(_PHANTOM_RESERVED_HEADER_PREFIX):
                logger.debug("stripping reserved phantom header from upstream: %s", name)
                continue
            rendered, header_ok = substitute(value, self._captures_as_dict(row.captured_values))
            if not header_ok:
                return TemplateUnresolved(placeholder=f"header[{name}]={value}")
            substituted_headers[name] = rendered

        body_bytes, body_content_type, sub_ok = self._render_body(
            step, row.captured_values, body_refs
        )
        if not sub_ok:
            return TemplateUnresolved(placeholder=f"body of step {step.name!r}")
        if body_content_type is not None and "Content-Type" not in substituted_headers:
            substituted_headers["Content-Type"] = body_content_type

        full_url = self._absolute_url(substituted_url, envelope)

        # (c) Inject auth via route policy.
        resolved = self._resolve_route(full_url, self._instance)
        if resolved.auth_mode == "phantom_bearer":
            slot = await self._cache.get(_hostname(full_url), row.uid)
            if slot is None or slot.status == "bad":
                return FailedAuth(status=401, observed_at=self._clock())
            substituted_headers["Authorization"] = slot.bearer

        # (d) Inject idempotency header.
        if step.idempotency_header:
            substituted_headers[step.idempotency_header] = envelope.idempotency_key

        # (e) Send via the transport. The resolved route may carry a
        # per-route timeout (§5.2) — pass it through; None falls back to
        # the upstream client's constructor default.
        request = UpstreamRequest(
            method=step.method,
            url=full_url,
            headers=substituted_headers,
            body=body_bytes,
            timeout_seconds=resolved.timeout_seconds,
        )
        try:
            response = await self._client.send(request)
        except httpx.HTTPError as exc:
            return FailedNetwork(error=str(exc))

        # (f) Classify.
        if 200 <= response.status < 300:
            new_captures = await self._extract_captures(step, response.body, row.captured_values)
            # finding R7-5-B: do NOT advance on the 2xx status ALONE. A 2xx
            # with a truncated/incomplete body (D-05/D-13, a CGNAT half-close,
            # a buggy proxy that returns 2xx before the body is whole) leaves
            # this step's declared captures unextracted. If a later step
            # REFERENCES one of those captures (``{{step.capture}}``), advancing
            # would strand it on an unresolvable ``None`` template FOREVER (the
            # row wedges in ``attempting`` holding a saturation slot — invariant
            # #1 bent). Validate the required captures were actually produced;
            # a missing one is a RETRYABLE failure, not a silent advance.
            missing = self._missing_required_captures(step, envelope, new_captures)
            if missing:
                logger.warning(
                    "step %r returned %d but is MISSING required capture(s) %s "
                    "(2xx with incomplete/truncated body); treating as retryable "
                    "rather than advancing into an unresolvable downstream step",
                    step.name,
                    response.status,
                    missing,
                )
                return CaptureIncomplete(
                    upstream_status=response.status,
                    missing_captures=missing,
                )
            chain_done = step_index + 1 >= len(envelope.steps)
            return Succeeded(
                captured=new_captures,
                next_step_index=step_index + 1,
                chain_done=chain_done,
                step_name=step.name,
                upstream_status=response.status,
                upstream_headers=response.headers,
            )
        if response.status in {401, 403}:
            if resolved.auth_mode == "phantom_bearer":
                # Mark the slot bad so the sender knows what to do.
                await self._cache.mark_bad(_hostname(full_url), row.uid)
            return FailedAuth(status=response.status, observed_at=self._clock())
        if 400 <= response.status < 500:
            return Failed4xx(status=response.status, body=response.body)
        if response.status >= 500:
            return Failed5xx(status=response.status)
        return FailedNetwork(error=f"Unexpected status {response.status}")

    def _check_capture_ttl(
        self,
        step: ChainStep,
        row: UploadRow,
        envelope: ChainEnvelope,
    ) -> CaptureExpiredStored | CaptureExpiredRewind | None:
        """Walk every placeholder this step references; gate on capture TTLs.

        If any referenced capture is missing/expired:
          - ``row.capture_reexecution_active is False`` → ``CaptureExpiredStored``.
          - True → rewind to the producing step (``CaptureExpiredRewind``).
        """
        placeholders: list[tuple[str, str]] = []
        placeholders.extend(find_placeholders(step.url))
        for v in step.headers.values():
            placeholders.extend(find_placeholders(v))
        body_template = self._body_as_template(step)
        if body_template:
            placeholders.extend(find_placeholders(body_template))
        if not placeholders:
            return None
        now = self._clock()
        for producing_step, capture_name in placeholders:
            step_values = row.captured_values.steps.get(producing_step)
            if step_values is None or capture_name not in step_values.values:
                # Not yet captured — substitute will detect; not a TTL miss.
                continue
            expires_at = step_values.expires_at.get(capture_name)
            if expires_at is None:
                continue  # non-expiring capture
            if expires_at > now:
                continue  # still valid
            # Expired.
            if row.capture_reexecution_active:
                # Find the index of the producing step in the envelope.
                rewind_index = next(
                    (i for i, s in enumerate(envelope.steps) if s.name == producing_step),
                    None,
                )
                if rewind_index is None:
                    # Producing step missing from envelope — fall back to stored.
                    return CaptureExpiredStored(producing_step=producing_step)
                return CaptureExpiredRewind(
                    producing_step=producing_step,
                    rewind_to_step_index=rewind_index,
                )
            return CaptureExpiredStored(producing_step=producing_step)
        return None

    @staticmethod
    def _captures_as_dict(
        captured: CapturedValues,
    ) -> dict[str, dict[str, Any]]:
        """Flatten :class:`CapturedValues` for the substitute helper."""
        return {name: dict(step.values) for name, step in captured.steps.items()}

    @staticmethod
    def _body_as_template(step: ChainStep) -> str | None:
        """Return the body content as a string for placeholder detection."""
        if isinstance(step.body, ChainBodyText):
            return step.body.value
        if isinstance(step.body, ChainBodyJson):
            return json.dumps(step.body.value)
        return None

    def _render_body(
        self,
        step: ChainStep,
        captured: CapturedValues,
        body_refs: dict[str, bytes],
    ) -> tuple[bytes, str | None, bool]:
        """Produce the outbound body bytes + Content-Type for ``step``.

        Returns:
            ``(bytes, content_type_or_None, all_resolved)``.
        """
        if step.body is None:
            return b"", None, True
        if isinstance(step.body, ChainBodyJson):
            value_template = json.dumps(step.body.value)
            rendered, ok = substitute(value_template, self._captures_as_dict(captured))
            if not ok:
                return b"", None, False
            return rendered.encode("utf-8"), "application/json", True
        if isinstance(step.body, ChainBodyText):
            rendered, ok = substitute(step.body.value, self._captures_as_dict(captured))
            if not ok:
                return b"", None, False
            return rendered.encode("utf-8"), step.body.content_type, True
        if isinstance(step.body, ChainBodyBytes):
            return (
                base64.b64decode(step.body.value_b64),
                step.body.content_type,
                True,
            )
        if isinstance(step.body, ChainBodyRef):
            data = body_refs.get(step.body.name)
            if data is None:
                return b"", None, False
            return data, step.body.content_type, True
        return b"", None, True  # pragma: no cover — exhaustive above

    def _absolute_url(self, url: str, envelope: ChainEnvelope) -> str:
        """Resolve ``url`` against ``envelope.default_target`` if needed."""
        if "://" in url:
            return url
        if envelope.default_target is None:
            return url
        base = str(envelope.default_target).rstrip("/")
        suffix = url if url.startswith("/") else "/" + url
        return base + suffix

    @staticmethod
    def _missing_required_captures(
        step: ChainStep,
        envelope: ChainEnvelope,
        captured: CapturedValues,
    ) -> tuple[str, ...]:
        """Return this step's REQUIRED captures that did not extract (finding R7-5-B).

        A capture this step DECLARES is "required" iff some LATER step
        references it via a ``{{step.capture}}`` placeholder (in its URL,
        headers, or body). Those are exactly the captures whose absence would
        wedge a downstream step on an unresolvable template. A required capture
        that extracted to ``None`` (no JSONPath match — a truncated/incomplete
        2xx body) is "missing".

        Captures this step declares but no later step references are NOT
        required: their absence cannot wedge anything, so a ``None`` there is
        tolerated (the step still succeeds). This keeps the validation precise
        — it fires exactly when an incomplete body would otherwise strand the
        chain, and never on a legitimately-absent optional field.

        Args:
            step: The step that just returned 2xx.
            envelope: The full chain (to scan LATER steps for references).
            captured: The merged captures after ``_extract_captures``.

        Returns:
            The names of this step's required-but-missing captures, in
            declaration order (empty when every required capture extracted).
        """
        declared = {c.name for c in step.capture}
        if not declared:
            return ()
        # Names of THIS step's captures referenced by any LATER step.
        step_index = next(
            (i for i, s in enumerate(envelope.steps) if s.name == step.name),
            None,
        )
        if step_index is None:  # pragma: no cover — step came from this envelope
            return ()
        referenced: set[str] = set()
        for later in envelope.steps[step_index + 1 :]:
            templates: list[str] = [later.url, *later.headers.values()]
            body_template = ChainExecutor._body_as_template(later)
            if body_template:
                templates.append(body_template)
            for template in templates:
                for producing_step, capture_name in find_placeholders(template):
                    if producing_step == step.name and capture_name in declared:
                        referenced.add(capture_name)
        if not referenced:
            return ()
        step_values = captured.steps.get(step.name)
        produced = step_values.values if step_values is not None else {}
        # Declaration order, restricted to required-and-missing.
        return tuple(
            c.name for c in step.capture if c.name in referenced and produced.get(c.name) is None
        )

    async def _extract_captures(
        self,
        step: ChainStep,
        body: bytes,
        existing: CapturedValues,
    ) -> CapturedValues:
        """Run JSONPath captures over ``body`` and merge into ``existing``.

        Emits a structured DEBUG log record carrying ``captures`` and
        ``sensitive_captures`` extras so
        :class:`phantom.observability.SensitiveCaptureRedactor` can redact
        values whose declaring :class:`ChainCapture` is ``sensitive=True``
        before the formatter sees them.
        """
        if not step.capture:
            return existing
        try:
            parsed_body = json.loads(body) if body else {}
        except json.JSONDecodeError:
            # Captures only apply to JSON responses; non-JSON yields empty extracts.
            parsed_body = {}
        values: dict[str, Any] = {}
        expires_at: dict[str, datetime | None] = {}
        captured_at = self._clock()
        sensitive_names: set[str] = set()
        for capture in step.capture:
            value = extract(parsed_body, capture.from_path)
            values[capture.name] = value
            if capture.ttl_seconds is not None:
                expires_at[capture.name] = captured_at + timedelta(seconds=capture.ttl_seconds)
            else:
                expires_at[capture.name] = None
            if capture.sensitive:
                sensitive_names.add(capture.name)
        # Emit a structured DEBUG log record. The captures dict is shared
        # by reference with the formatter; the redaction filter mutates
        # the copy passed in extras (we copy first so the persisted
        # ``CapturedStepValues.values`` keeps the raw value for admin-API
        # consumers). Gated on isEnabledFor so production INFO-level
        # deployments pay zero per-step cost for the structured-extra
        # dict construction.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "chain step captured values",
                extra={
                    "captures": {step.name: dict(values)},
                    "sensitive_captures": {step.name: set(sensitive_names)},
                },
            )
        step_values = CapturedStepValues(
            values=values,
            captured_at=captured_at,
            expires_at=expires_at,
        )
        merged_steps = dict(existing.steps)
        merged_steps[step.name] = step_values
        return CapturedValues(steps=merged_steps)


def _hostname(url: str) -> str:
    """Hostname helper used by the executor for cache lookups."""
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()


def default_clock() -> datetime:
    """Default UTC clock used in production wiring."""
    return datetime.now(tz=UTC)
