"""Sender claim batch size is the named constant (cycle-7 task 6.2, D6).

The worker loop's ``claim_due(now, limit=...)`` must reference
``CLAIM_BATCH_SIZE`` rather than a bare literal, so the one-row-per-poll
decision stays attached to its rationale comment. A structural AST gate
pins the call site; a value test pins the current contract (one row per
poll; concurrency comes from the worker pool).
"""

from __future__ import annotations

import ast
from pathlib import Path

from phantom.workers.sender import CLAIM_BATCH_SIZE

_SENDER_PATH = Path(__file__).parent.parent.parent / "src" / "phantom" / "workers" / "sender.py"


def test_claim_batch_size_is_one_row_per_poll() -> None:
    """One claimed row per poll: extras would park in ``attempting``."""
    assert CLAIM_BATCH_SIZE == 1


def test_claim_due_limit_references_the_constant() -> None:
    """Every ``claim_due`` call in the sender passes ``limit=CLAIM_BATCH_SIZE``.

    A bare numeric ``limit`` is the regression this gate forbids: the
    batch size must stay a named, rationale-carrying constant.
    """
    tree = ast.parse(_SENDER_PATH.read_text())
    claim_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "claim_due"
    ]
    assert claim_calls, "expected at least one claim_due call site in workers/sender.py"
    for call in claim_calls:
        limit_kwargs = [kw for kw in call.keywords if kw.arg == "limit"]
        assert limit_kwargs, "claim_due must pass limit as a keyword"
        for kw in limit_kwargs:
            assert isinstance(kw.value, ast.Name) and kw.value.id == "CLAIM_BATCH_SIZE", (
                "claim_due's limit must reference CLAIM_BATCH_SIZE, not a bare literal"
            )
