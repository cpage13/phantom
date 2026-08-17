"""N1 — one decode rule for inline base64, enforced at admission and at send.

``ChainBodyBytes.value_b64`` carried no validation beyond being a string, and
the executor decoded it with a bare ``base64.b64decode``. The decoder raises two
DIFFERENT exception types depending on the input, and the sender's worker loop
catches only ``sqlite3.OperationalError``, so either one escaped, killed the
sender TaskGroup, and was re-claimed first on every restart: one malformed
producer payload permanently disabled the service.

N1 is two layers over one shared decoder. Admission rejects the payload with a
422 ``envelope_invalid`` so no such row is ever admitted; the executor
classifies it as ``InlineBodyInvalid`` so rows admitted before the guard existed
(or inserted by any other path) terminate as ``failed`` rather than killing the
worker. ``envelope_from_persistence_json`` re-validates the envelope's SHAPE but
never runs the static passes, which is exactly why the second layer is needed.

**The parametrisation set is load-bearing.** ``b64decode`` accepts ``str`` and
encodes it to ASCII first, so any non-ASCII character raises the bare parent
exception BEFORE base64 decoding is attempted, while malformed base64 raises the
library's own subclass. A guard that catches only the subclass passes the first
three cases below and reopens the crash loop on the last two, so a test set
without them proves nothing about the half of the fix that matters.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.chain.parser import ParserError, decode_inline_body_b64, parse_json_request
from phantom.config.settings import InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

# Both decoder failure classes. The first three raise the library's own
# malformed-base64 error; the last two are non-ASCII and raise the bare parent
# class, which is the round-2 blocking finding this set exists to pin.
_MALFORMED_B64 = [
    "A",  # 1 more than a multiple of 4
    "AAAAA",  # 5 characters
    "ab=c",  # incorrect padding
    "é",  # non-ASCII, never reaches base64 decoding
    "ABé=",  # non-ASCII mixed with valid characters
]

# One case from each class, for the tests that drive the whole sender.
_ONE_PER_CLASS = ["A", "é"]

# The host the test instance routes, and the step name every envelope uses.
ROUTED_HOST = "files.example.com"
STEP_NAME = "inline_body_step"

# Declared size for the saturation assertion in test 4.
_DECLARED_BYTES = 512


class _FakeUpstream:
    """Stub upstream client. No test here should reach the transport."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a real-store instance whose one route matches ``files.example.com``.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The tracked :class:`InstanceContext`.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[ROUTED_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="files", hosts=[ROUTED_HOST], auth_mode="none")],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    return track_instance(instance)


def _envelope_json(chain_id: UUID, *, value_b64: str) -> bytes:
    """Build a one-step envelope whose inline body carries ``value_b64``.

    Args:
        chain_id: The chain's identity.
        value_b64: The producer-supplied base64 text under test.

    Returns:
        The envelope as JSON bytes.
    """
    return json.dumps(
        {
            "chain_id": str(chain_id),
            "idempotency_key": "k",
            "steps": [
                {
                    "name": STEP_NAME,
                    "method": "PUT",
                    "url": f"https://{ROUTED_HOST}/v1/upload",
                    "body": {"kind": "bytes", "value_b64": value_b64},
                }
            ],
        }
    ).encode("utf-8")


def _persisted_row(chain_id: UUID, *, value_b64: str, body_size_bytes: int = 0) -> UploadRow:
    """Build an ``attempting`` row carrying a malformed envelope, bypassing admission.

    The envelope JSON is assigned to ``chain_envelope_json`` directly rather than
    round-tripped through ``parse_json_request``, because post-fix the parser is
    exactly what rejects these payloads. This is the already-admitted-row case
    the depth layer exists for.

    Args:
        chain_id: The chain's identity and the row's primary key.
        value_b64: The malformed base64 text.
        body_size_bytes: Declared size, for the saturation assertion.

    Returns:
        The persisted-shape :class:`UploadRow`.
    """
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": "attempting",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": ROUTED_HOST,
            "uid": "user-1",
            "chain_envelope_json": _envelope_json(chain_id, value_b64=value_b64).decode("utf-8"),
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": body_size_bytes,
            "storage_encoding": "original",
        },
    )


@pytest.mark.parametrize("value_b64", _MALFORMED_B64)
async def test_malformed_inline_base64_is_rejected_at_parse(value_b64: str) -> None:
    """No row is ever admitted for an undecodable inline body, for either class.

    Objective: the primary layer. Success: ``parse_json_request`` raises
    ``ParserError`` with the ``envelope_invalid`` code and a message naming the
    step, for all five cases.
    """
    with pytest.raises(ParserError) as exc_info:
        await parse_json_request(
            _envelope_json(uuid4(), value_b64=value_b64),
            instance_id="primary",
            request_id="r",
            max_buffered_bytes=10_000,
        )

    assert exc_info.value.code == "envelope_invalid"
    assert STEP_NAME in exc_info.value.message
    # The offending value is producer data of unbounded size; it must not be
    # echoed back in the error.
    assert value_b64 not in exc_info.value.message


async def test_standard_and_newline_wrapped_base64_are_accepted() -> None:
    """Counter-test pinning the decode parameters: legitimate output must not be rejected.

    Objective: the guard must accept both plain ``b64encode`` output and
    MIME-style newline-wrapped ``encodebytes`` output, which is what forbids
    ``validate=True``.

    Success: both envelopes parse, both decode back to the original payload, and
    the wrapped form provably WOULD be rejected under ``validate=True``, which
    records why the prohibition exists rather than leaving a bare instruction.
    """
    payload = b"phantom-n1-legitimate-producer-bytes" * 8
    plain = base64.b64encode(payload).decode()
    wrapped = base64.encodebytes(payload).decode()
    assert "\n" in wrapped, "encodebytes must produce the newline-wrapped form under test"

    for value in (plain, wrapped):
        envelope, _ = await parse_json_request(
            _envelope_json(uuid4(), value_b64=value),
            instance_id="primary",
            request_id="r",
            max_buffered_bytes=100_000,
        )
        assert envelope.steps[0].name == STEP_NAME
        assert decode_inline_body_b64(value, step_name="s") == payload

    with pytest.raises(ValueError):
        base64.b64decode(wrapped, validate=True)


@pytest.mark.parametrize("value_b64", _MALFORMED_B64)
async def test_executor_classifies_a_persisted_malformed_inline_body(
    tmp_path: Path, value_b64: str
) -> None:
    """The depth layer covers rows that predate the admission guard.

    Objective: ``envelope_from_persistence_json`` re-validates shape but not
    base64, so a row inserted before N1 (or by any other path) must classify
    rather than raise. Success: ``execute_one_step`` returns an
    ``InlineBodyInvalid`` naming the step, for all five cases.

    The import is inside the body so this module collects on a tree where the
    variant does not exist yet, which is what lets test 1 fail behaviourally.
    """
    from phantom.chain.executor import InlineBodyInvalid

    instance = await _build_instance(tmp_path)
    row = _persisted_row(uuid4(), value_b64=value_b64)

    result = await instance.executor.execute_one_step(row, {})

    assert isinstance(result, InlineBodyInvalid), (
        f"an undecodable persisted body must classify, not raise; got {type(result).__name__}"
    )
    assert result.step_name == STEP_NAME
    assert result.reason


@pytest.mark.parametrize("value_b64", _ONE_PER_CLASS)
async def test_drive_one_terminates_a_malformed_inline_body_row_as_failed(
    tmp_path: Path, value_b64: str
) -> None:
    """The sender resolves the row instead of dying, and releases its slot.

    Objective: the whole path, for one case from each exception class. Success:
    ``_drive_one`` returns normally, the re-read row is terminal ``failed`` with
    the short stable token, and the saturation slot admitted before the drive is
    back.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    row = _persisted_row(chain_id, value_b64=value_b64, body_size_bytes=_DECLARED_BYTES)
    await instance.store.insert(row)
    admitted = await instance.saturation.admit(_DECLARED_BYTES)
    assert admitted.__class__.__name__ == "AdmissionGranted", admitted

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "failed"
    assert fresh.last_error == f"inline_body_invalid:{STEP_NAME}"
    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0


def test_the_decode_rule_has_exactly_one_definition() -> None:
    """Pin the single-definition design: one decode rule, one exception, no reach-around.

    Objective: stop a later edit reintroducing a second decode with different
    parameters or a narrower catch, which would split the admission rule and the
    send rule apart. A payload admission accepted but the executor could not
    render would be noisy; a payload admission rejected that WAS deliverable
    would be data loss.

    These are LITERAL text scans, and the middle one is the strictest rule in
    the phase: it forbids the library exception's name in comments as well as in
    code, anywhere outside the parser. That is what the decoder-owned exception
    exists to make true: one file knows the taxonomy, every caller knows one
    name.
    """
    service_src = Path(__file__).resolve().parents[2] / "src" / "phantom"
    parser = service_src / "chain" / "parser.py"
    executor = service_src / "chain" / "executor.py"

    assert "b64decode(" not in executor.read_text(encoding="utf-8"), (
        "the executor must reach the decoder through the parser, not call b64decode itself"
    )

    # Named here (this file is outside the scanned tree) so the rule is legible.
    library_exception = "binascii"
    offenders = [
        path.relative_to(service_src)
        for path in service_src.rglob("*.py")
        if path != parser and library_exception in path.read_text(encoding="utf-8")
    ]
    assert offenders == [], (
        f"only chain/parser.py may name the library decode exception, and only in its "
        f"docstrings; found it in {offenders}"
    )

    assert parser.read_text(encoding="utf-8").count("b64decode(") == 1, (
        "the parser must hold exactly one b64decode call, the single decode rule"
    )
