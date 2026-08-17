"""The single writer of ``new_state="expired"`` (ADR-032, leaf module B3).

Every other sender-owned state transition writes its ``new_state="…"``
literal inside :mod:`phantom.workers.sender` (ADR-015). ``expired`` is the
one exception: it is fired from TWO subsystems — the executor-driven sender
give-up path (``_on_send_deadline_expired``) AND the kicker parked-row sweeps
— and both must apply identical body-discard + saturation-release + CAS
semantics. Centralising the write in this one cycle-free leaf module keeps the
ADR-015 one-writer-per-effect discipline (exactly one ``new_state="expired"``
call site) while spanning the two callers.

This module is intentionally dependency-light: it depends only on the
:class:`~phantom.storage.interface.UploadStore` and
:class:`~phantom.storage.interface.BodyStore` protocols and the
:class:`~phantom.workers.saturation.SaturationGate`, all of which every caller
already holds via its :class:`InstanceContext`. Both protocols live in the same
``storage.interface`` module, so adding the body store costs no new dependency
edge. :mod:`phantom.workers.sender` does not import the kickers and the kickers
do not import the sender, so a leaf module imported by all three creates no
cycle.

The send-deadline TRANSITION sites that CALL this writer (the executor gate and
the parked-``auth_expired`` sweeps) are added separately; until they land,
``expire_row`` has no callers and nothing produces the ``expired`` state.

See:
- ADR-032: the ``expired`` terminal state.
- ADR-015: state transitions owned by the sender (and why this one writer
  spans two callers).
"""

from __future__ import annotations

import logging

from phantom.models.upload import UploadRow, UploadState
from phantom.storage.interface import BodyStore, UploadStore
from phantom.workers.saturation import SaturationGate

logger = logging.getLogger(__name__)


async def expire_row(
    store: UploadStore,
    saturation: SaturationGate,
    body_store: BodyStore,
    row: UploadRow,
    *,
    expected_state: UploadState,
    last_error: str,
    upstream_status: int | None,
    release_saturation: bool,
) -> None:
    """Transition a row to the terminal ``expired`` state: dead, discard the body.

    UNLIKE ``_record_stored`` (which RETAINS the body and deliberately HOLDS
    the saturation slot for replay), ``expired`` is terminal-dead: it discards
    the body (design §6.3 / ADR-032) in BOTH caller paths. The discard is two
    halves and both happen here: the row-side stamp plus accounting zero
    through ``discard_body_and_zero_accounting``, and the byte-side
    ``body_store.delete``. Before F3 only the first half ran, so a RAM body
    survived for the process lifetime while the row said it was gone,
    unreachable by the reaper (its body pass filters on the stamp), by
    ``RamBodyStore.list_orphans`` (it returns ``[]``), and by the
    ``PersistController`` (it skips stamped rows).

    ORDERING: stamp first, delete only after a confirmed flip, mirroring the
    reaper's R9-5 leg and the sender's R10-1 leg. Never delete bytes this call
    did not stamp. The crash window between the two is bounded and accepted: a
    crash frees a RAM body outright, and a disk body's files stay invisible to
    both reclaim mechanisms only until the metadata-retention pass deletes the
    row, after which the orphan janitor reclaims them. Those bytes are still
    counted against the disk ceiling by the store's live tree walk, so the
    ENOSPC gate is not fooled. The alternative ordering is worse in kind: a
    delete that crashes before the stamp leaves an UNSTAMPED row with no bytes,
    which the next claim lands in a false ``corrupted``.

    The saturation release depends on whether the row STILL HOLDS its slot at
    the moment it expires, and that differs by path:

    * **Path A (executor give-up gate).** The row is ``attempting`` — it still
      holds the slot it was admitted with. Expiring it must RELEASE that slot
      (``release_saturation=True``). No single existing terminal path both
      discards and releases in this shape, so the two legs are composed here in
      the correct order: flip the state under a CAS guard, discard the body,
      then release the slot off the discard outcome's in-transaction pre-zero
      size (never the stale ``row.body_size_bytes``).
    * **Path B (kicker parked-row sweep).** The row is ``auth_expired`` — its
      slot was ALREADY released at park time by ``_on_auth_failure`` (the body
      was RETAINED, the accounting zeroed). Releasing again here would
      double-free: ``SaturationGate.release`` floors at zero so the counters
      never go negative, but under a concurrent in-flight row the extra
      decrement transiently UNDER-counts ``in_flight``, briefly admitting one
      upload past the saturation cap. So the sweep passes
      ``release_saturation=False`` — body still discarded, slot NOT re-released.

    Args:
        store: The upload store owning the row's metadata.
        saturation: The instance saturation gate to release on body discard.
        body_store: The instance's mode-selected body store, whose
            ``delete`` frees the bytes themselves. ``delete`` is idempotent on
            both halves of :class:`HybridBodyStore`, so this covers ``ram`` and
            ``file`` ``body_location`` without branching.
        row: The claimed/parked row being expired.
        expected_state: The CAS pre-state the ``record_attempt_result`` UPDATE
            guards on — ``"attempting"`` for the executor give-up path, or
            ``"auth_expired"`` for the parked-row kicker sweep. A row that moved
            under us (admin cancel/replay or a concurrent wake) yields
            rowcount 0 and is left untouched, not clobbered.
        last_error: One-line summary persisted on the row (e.g.
            ``"send_deadline:Ns"``).
        upstream_status: The most recent upstream status, or ``None``.
        release_saturation: Whether the row HOLDS a saturation slot at expiry.
            ``True`` for path A (the ``attempting`` row still holds its admit);
            ``False`` for path B (the parked ``auth_expired`` row's slot was
            already released at park). The body is discarded regardless; only
            the slot release is gated.
    """
    rowcount = await store.record_attempt_result(
        row.chain_id,
        new_state="expired",
        attempts=row.attempts,
        next_attempt_at=None,
        last_error=last_error,
        upstream_status=upstream_status,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state=expected_state,  # caller-correct CAS guard
    )
    if rowcount == 0:
        # A concurrent admin cancel/replay or a kicker wake moved the row
        # between claim and this write; do NOT clobber the new state.
        logger.info("expire_row no-op: chain_id=%s — row moved under us", row.chain_id)
        return
    # DISCARD FIRST — capture the in-transaction pre-zero size as the release
    # basis (NEVER the stale ``row.body_size_bytes`` snapshot). The row is NOW
    # in ``expired`` (we just flipped it), so the discard CAS guards on that.
    # The body is discarded in BOTH paths; only the slot release below differs.
    outcome = await store.discard_body_and_zero_accounting(row.chain_id, expected_state="expired")
    # RELEASE only on path A (``release_saturation``), AND only if THIS call
    # zeroed the slot (``outcome.flipped``), using the OUTCOME's pre-zero size.
    # ``outcome.flipped`` alone is NOT enough: a path-B ``auth_expired`` row's
    # body is still present at sweep time, so the discard DOES flip — but its
    # saturation slot was already released at park (``_on_auth_failure``), so
    # releasing here would double-free. ``release_saturation`` distinguishes the
    # holds-a-slot path (A) from the already-released park path (B); the
    # ``flipped`` guard still defends path A against a concurrent mover.
    if not outcome.flipped:
        # Another owner moved or already stamped this row between the state
        # flip and here (an admin replay, a concurrent kicker tick, or a
        # reaper discard). Whoever stamped it owns its bytes. Touch nothing.
        return
    if release_saturation:
        await saturation.release(outcome.body_size_bytes)
    # F3: the bytes themselves, not just the accounting. ADR-032 says an
    # expired row's body is discarded at the transition; before this fix
    # only the row-side stamp happened, so RAM bodies survived for the
    # process lifetime, unreachable by the reaper (it filters on the stamp),
    # by RamBodyStore.list_orphans (it returns []), and by the
    # PersistController (it skips stamped rows). Delete is idempotent on
    # both halves of HybridBodyStore, so this covers ram and file
    # body_location without branching. Released BEFORE the delete because a
    # delete that raises (a permission fault inside the disk half's _rm_rf)
    # would otherwise leak a slot permanently, which eventually 503s all
    # ingress; leaking bytes instead is bounded by the retention windows.
    await body_store.delete(row.chain_id)
