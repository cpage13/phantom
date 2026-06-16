"""Falsifiability: admission's atomic-transaction property.

Plan § 2.3.17 (H7 closure): the upload row INSERT + the
idempotency_index INSERT land in ONE SQLite transaction. A crash
between the two must leave neither persisted.

This script asserts the contract by inspecting the source: the
``insert_with_idempotency_claim`` method on
``SqliteUploadStore`` must use an explicit ``BEGIN`` /
``commit`` / ``rollback`` block wrapping BOTH inserts. Any other
ordering (or a missing BEGIN) is a defect.

The runtime regression test for the same property lives at
``tests/e2e/crash_recovery/test_crash_admission_atomic.py``; this
script is a complementary static check that catches regressions
in the source-level commitment without requiring a test run.

Exits:
- 0: the source contains the documented BEGIN/commit/rollback shape.
- 1: the source has drifted (no BEGIN, or rollback missing, or the
     two INSERTs are not wrapped together).
- 2: the source file is missing.

Run via: ``uv run python scripts/check_atomic_admission.py``
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_STORE_PY = (
    _REPO_ROOT / "src" / "phantom-service" / "src" / "phantom" / "storage" / "sqlite_store.py"
)
_METHOD_NAME = "insert_with_idempotency_claim"


def main() -> int:
    """Entry point for the atomic-admission contract check."""
    if not _STORE_PY.exists():
        sys.stderr.write(f"missing source file: {_STORE_PY}\n")
        return 2

    src = _STORE_PY.read_text()

    # Locate the method body. Match the def line, then everything
    # until the next top-level def at the same column depth.
    method_re = re.compile(
        rf"(\s+)async def {_METHOD_NAME}\(.*?(?=\n\1(?:async )?def |\nclass |\Z)",
        re.DOTALL,
    )
    match = method_re.search(src)
    if match is None:
        sys.stderr.write(
            f"FALSIFIED: {_METHOD_NAME!r} method not found in {_STORE_PY.relative_to(_REPO_ROOT)}\n"
        )
        return 1
    body = match.group(0)

    # Required atomic-transaction shape:
    #   await conn.execute("BEGIN")
    #   await conn.execute(... INSERT INTO uploads ...)
    #   await conn.execute(... INSERT INTO idempotency_index ...)
    #   await conn.commit()
    #   except sqlite3.IntegrityError ...:
    #       await conn.rollback()
    required = [
        r'await\s+conn\.execute\(\s*"BEGIN"\s*\)',
        r"INSERT INTO uploads",
        r"INSERT INTO idempotency_index",
        r"await\s+conn\.commit\(\)",
        r"sqlite3\.IntegrityError",
        r"await\s+conn\.rollback\(\)",
    ]
    missing: list[str] = []
    for pattern in required:
        if not re.search(pattern, body):
            missing.append(pattern)

    if missing:
        sys.stderr.write(
            "FALSIFIED: insert_with_idempotency_claim has drifted from the "
            "H7-closure atomic-transaction shape. Missing fragments:\n"
        )
        for pat in missing:
            sys.stderr.write(f"  - /{pat}/\n")
        return 1

    # Ordering check: BEGIN must appear before both INSERTs; commit
    # must appear after both INSERTs.
    begin_pos = body.find('execute("BEGIN")')
    uploads_pos = body.find("INSERT INTO uploads")
    idem_pos = body.find("INSERT INTO idempotency_index")
    commit_pos = body.find("await conn.commit()")
    if not (begin_pos < uploads_pos < idem_pos < commit_pos):
        sys.stderr.write(
            "FALSIFIED: ordering drift — atomic-transaction sequence "
            "is not BEGIN → uploads INSERT → idempotency_index INSERT → commit.\n"
        )
        return 1

    print(f"OK: {_METHOD_NAME} preserves the H7-closure atomic-transaction shape.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
