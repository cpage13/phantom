"""Single-writer-per-purpose invariants (plan § 0.5 / § 2.3.21 #7).

Per the single-writer manifest in plan § 0.5, each named coroutine
has exactly one write-purpose against ``uploads``. Two of those are
load-bearing enough that Slice 1.F asserts them via grep on the
production source tree:

* **Invariant #6** — only :class:`PersistController` writes
  ``UPDATE uploads SET body_location='file'``. The transition
  ``ram -> file`` is the durability commit point; spreading the
  write across multiple sites defeats the single-writer guarantee.
* **Reaper write-purpose** — :class:`Reaper` writes only
  ``body_discarded_at`` (via :meth:`UploadStore.discard_body`) and
  the terminal-row DELETE (via
  :meth:`UploadStore.delete_terminal_older_than`). No other ``UPDATE
  uploads`` lives in :mod:`phantom.workers.reaper`.
* **Watchers / janitors abstain** — :class:`RamPressureWatcher`,
  :class:`BodyOrphanJanitor`, :class:`DiskPressureProbe` do NOT
  write to ``uploads``. They enqueue against the controller, sweep
  the body store, or sample disk usage.

Grep is the structural enforcement at the SOURCE-TREE level. Phase 3
adds a runtime invariant-audit coroutine that asserts the same
property by walking live rows + counters.
"""

from __future__ import annotations

import re
from pathlib import Path

# tests/unit/test_invariants.py lives 3 parents up from src/phantom-service/, so
# parents[2] is src/phantom-service/ and we descend into src/phantom-service/src/phantom/.
_PHANTOM_PKG_ROOT = Path(__file__).resolve().parents[2]
_PHANTOM_SRC = _PHANTOM_PKG_ROOT / "src" / "phantom"
_WORKERS_DIR = _PHANTOM_SRC / "workers"


def _read(path: Path) -> str:
    """Read a source file's content as text."""
    return path.read_text(encoding="utf-8")


def _strip_comments_and_docstrings(src: str) -> str:
    """Remove '#' comments + triple-quoted strings for invariant grep.

    Documentation routinely mentions ``mark_persisted`` /
    ``body_location='file'`` outside the actual call site (cross-
    reference text). The grep must target executable code; this
    helper strips the obvious doc surfaces — ``#``-comments and
    triple-quoted strings — so the assertion is not tripped by
    explanatory prose.
    """
    # Drop triple-quoted strings (both flavors).
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    # Drop full-line comments.
    src = re.sub(r"^\s*#.*$", "", src, flags=re.MULTILINE)
    # Drop inline trailing comments.
    src = re.sub(r"\s+#.*$", "", src, flags=re.MULTILINE)
    return src


def test_only_persist_controller_calls_mark_persisted() -> None:
    """Grep the workers/ tree — only ``persist_controller.py`` calls ``mark_persisted``.

    Invariant #6 closure (plan § 0.5). Any new caller of
    :meth:`UploadStore.mark_persisted` outside the persist controller
    is a single-writer violation.
    """
    callers: list[Path] = []
    for worker in _WORKERS_DIR.glob("*.py"):
        body = _strip_comments_and_docstrings(_read(worker))
        if re.search(r"\bmark_persisted\s*\(", body):
            callers.append(worker.relative_to(_PHANTOM_SRC))
    expected = [Path("workers/persist_controller.py")]
    assert callers == expected, (
        f"Single-writer violation (invariant #6): only persist_controller.py "
        f"may call ``mark_persisted``. Other callers found: {callers}"
    )


def test_only_sqlite_store_emits_the_body_location_update() -> None:
    """The ``UPDATE uploads SET body_location='file'`` SQL lives in ONE place.

    The literal SQL must appear only in
    :mod:`phantom.storage.sqlite_store` (the store's ``mark_persisted``
    implementation). Production callers go through the method; raw
    SQL emitted from a worker (or any other module) is a violation
    that the pre-commit ``forbid-out-of-order-persist`` grep would
    also catch.
    """
    pattern = re.compile(r"SET\s+body_location\s*=\s*['\"]file['\"]")
    hits: list[Path] = []
    for path in _PHANTOM_SRC.rglob("*.py"):
        body = _strip_comments_and_docstrings(_read(path))
        if pattern.search(body):
            hits.append(path.relative_to(_PHANTOM_SRC))
    expected = [Path("storage/sqlite_store.py")]
    assert hits == expected, (
        f"Single-writer violation: ``UPDATE uploads SET body_location='file'`` "
        f"SQL must only live in storage/sqlite_store.py. Other emitters: {hits}"
    )


def test_reaper_writes_only_discard_body_and_delete_terminal() -> None:
    """Reaper's ``uploads``-mutating calls are bounded.

    The reaper's contract (plan § 0.5 single-writer manifest) on the
    ``uploads`` table: ``UPDATE body_discarded_at`` (via
    :meth:`UploadStore.discard_body_and_zero_accounting`, the scheduled
    leg of the one body-discard operation; the sender's immediate leg
    calls the SAME op), DELETE terminal rows past
    retention (via :meth:`UploadStore.delete_terminal_older_than`), and
    the V3 count-cap backstop DELETE of oldest-terminal rows over
    ``retention.max_rows`` (via
    :meth:`UploadStore.evict_terminal_over_limit`). All three are
    retention deletes/discards — the reaper's documented purpose. It
    additionally manages the separate ``idempotency_index`` table via
    :meth:`UploadStore.cleanup_idempotency_index` — that write is on a
    different table and is the reaper's documented secondary purpose
    (plan § 2.3.16). Any other ``store.*`` write-shaped call from the
    reaper is a violation.
    """
    body = _strip_comments_and_docstrings(_read(_WORKERS_DIR / "reaper.py"))
    # Allow-listed ``uploads`` writes (retention discards/deletes incl.
    # the V3 max_rows backstop) + the reaper's idempotency_index
    # secondary purpose (different table; still single-purpose per
    # caller).
    allowed = {
        "discard_body_and_zero_accounting",
        "delete_terminal_older_than",
        "evict_terminal_over_limit",
        "cleanup_idempotency_index",
    }
    # Find every ``store.<name>(`` mutation-shaped call. Known reads
    # like ``list_terminal_older_than`` and ``list_chain_ids`` are
    # excluded so the allow list narrows to writers only. ``get`` is
    # the eviction pass's live-row re-read guarding its late body
    # delete (R10-D1) - a read, not a write.
    known_reads = {"list_terminal_older_than", "list_chain_ids", "get"}
    mutations = {
        m.group(1)
        for m in re.finditer(r"\bstore\.([a-z_][a-z0-9_]*)\(", body)
        if m.group(1) not in known_reads
    }
    violations = mutations - allowed
    assert not violations, (
        f"Reaper write-purpose violation: only {sorted(allowed)} are allowed; "
        f"found extra store mutators: {sorted(violations)}"
    )


def test_watchers_and_janitors_do_not_write_uploads() -> None:
    """RamPressureWatcher, BodyOrphanJanitor, DiskPressureProbe — read-only on ``uploads``.

    These workers signal the persist controller (enqueue) or sweep the
    body store; they NEVER mutate the upload-row metadata. Any
    ``store.update_*`` / ``store.mark_*`` / ``store.discard_*`` /
    ``store.delete_*`` call in their bodies is a violation.
    """
    write_pattern = re.compile(
        r"\bstore\.(update_|mark_|discard_|delete_|record_|insert|claim_|replay|cancel|bulk_delete)"
    )
    targets = {
        "ram_pressure.py",
        "body_orphan_janitor.py",
        "disk_pressure.py",
    }
    violations: dict[str, list[str]] = {}
    for name in targets:
        path = _WORKERS_DIR / name
        if not path.exists():
            continue
        body = _strip_comments_and_docstrings(_read(path))
        hits = [m.group(0) for m in write_pattern.finditer(body)]
        if hits:
            violations[name] = hits
    assert not violations, f"Watcher/janitor must not mutate ``uploads``: {violations}"
