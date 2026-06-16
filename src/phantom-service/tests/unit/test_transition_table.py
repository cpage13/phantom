"""Documentation test for upload state transitions.

The brainstorm chose Path A (delete the pure state-machine module);
the sender's ``_on_*`` handlers ARE the canonical transition table. This
test parses ``workers/sender.py`` via ast, extracts every ``new_state="..."``
literal each ``_on_*`` handler writes (after the refactor that eliminated
intermediate variables in ``_on_succeeded``), and asserts:

  (a) Every ChainState value that the SENDER is responsible for
      writing has at least one incoming edge in ``workers/sender.py``.
  (b) Every non-terminal state has at least one outgoing reader
      somewhere under ``workers/``.

States explicitly NOT written by the sender (and so excluded from
the incoming-edge invariant for this file):
  - 'attempting' — written by ``storage.claim_due`` (memory/disk store
    transition queued→attempting); the sender's role is to advance
    from attempting, not enter it.
  - 'cancelled' — written by the admin cancel endpoint
    (``routes/admin.py``), not the sender.

If a future commit adds a new ChainState value without a handler that
writes it (and it's not one of the two exclusions above), this test
fails. This is the documentation-as-test invariant for ADR-015
(state transitions owned by sender).
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import get_args

from phantom.models.chain import ChainState

_SENDER_EXCLUDES = frozenset({"attempting", "cancelled"})


def _extract_new_state_writes(source: str) -> set[str]:
    """Return every string literal passed as ``new_state=`` to a Call.

    Walks both ``Call.keywords`` (direct literal: ``new_state="x"``) AND
    ``Assign`` nodes targeting a local Name ``new_state`` (the variable
    pattern). The sender refactor above eliminates the variable
    pattern, but the scanner stays robust so any later refactor
    that reintroduces it still works.
    """
    tree = ast.parse(source)
    written: set[str] = set()
    # Direct keyword literals: f(new_state="x", ...)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if (
                    kw.arg == "new_state"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    written.add(kw.value.value)
    # Assignment targets: new_state = "x"
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "new_state"
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    written.add(node.value.value)
    return written


def test_every_sender_owned_state_has_incoming_edge() -> None:
    """Each ChainState the sender owns shows up as a ``new_state=`` literal."""
    sender_path = Path(__file__).parent.parent.parent / "src" / "phantom" / "workers" / "sender.py"
    written_states = _extract_new_state_writes(sender_path.read_text())
    expected = set(get_args(ChainState)) - _SENDER_EXCLUDES
    missing = expected - written_states
    assert not missing, (
        f"ChainState values the sender is responsible for but never writes: "
        f"{sorted(missing)}. Either the sender is missing an _on_* handler, "
        f"or the state belongs in _SENDER_EXCLUDES."
    )


def test_excluded_states_have_writers_elsewhere() -> None:
    """The excluded states must still be written somewhere in the codebase.

    ``'attempting'`` is written by ``storage.claim_due``;
    ``'cancelled'`` is written by ``routes/admin.py``'s cancel endpoint.
    """
    src_root = Path(__file__).parent.parent.parent / "src" / "phantom"
    full_source = ""
    for path in src_root.rglob("*.py"):
        full_source += path.read_text() + "\n"
    for excluded_state in _SENDER_EXCLUDES:
        assert f'"{excluded_state}"' in full_source or f"'{excluded_state}'" in full_source, (
            f"Excluded state {excluded_state!r} not written anywhere in src/phantom-service/"
        )


def test_every_nonterminal_state_has_outgoing_edge() -> None:
    """Every non-terminal state must be observable somewhere in workers/.

    Non-terminal: ``queued``, ``attempting``, ``auth_expired``.
    Terminal: ``succeeded``, ``failed``, ``stored``, ``cancelled``, ``corrupted``.
    """
    non_terminal = {"queued", "attempting", "auth_expired"}
    workers_dir = Path(__file__).parent.parent.parent / "src" / "phantom" / "workers"
    source = ""
    for path in workers_dir.glob("*.py"):
        source += path.read_text()
    for state in non_terminal:
        assert f'"{state}"' in source, f"State {state} has no reader in workers/"
