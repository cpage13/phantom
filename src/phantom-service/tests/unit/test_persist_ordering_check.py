"""Unit test for the pre-commit ``check_persist_ordering.py`` script.

Plan § 4.2.6. Exercises both halves of the check:

1. ``mark_persisted`` calls outside ``workers/persist_controller.py``
   are flagged.
2. Inside ``persist_controller.py`` the file-store put call MUST
   precede the ``mark_persisted`` call (commit-last-column ordering).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_persist_ordering.py"


def _load_script_module() -> ModuleType:
    """Load the script as a module so its functions are unit-testable."""
    spec = importlib.util.spec_from_file_location("check_persist_ordering", _SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_persist_ordering"] = module
    spec.loader.exec_module(module)
    return module


def test_script_passes_on_current_codebase() -> None:
    """The current production code satisfies both invariants."""
    module = _load_script_module()
    violations: list[str] = []
    violations.extend(module.check_mark_persisted_call_sites())
    violations.extend(module.check_persist_controller_internal_ordering())
    assert violations == [], (
        "Persist-handoff ordering check should pass on the production tree; "
        f"violations: {violations}"
    )


def test_ast_call_sites_helper_distinguishes_def_from_call(tmp_path: Path) -> None:
    """AST walk distinguishes ``def mark_persisted`` from a call expression."""
    module = _load_script_module()

    # File with only a method def — no call site.
    only_def = tmp_path / "only_def.py"
    only_def.write_text(
        "class S:\n    async def mark_persisted(self, x):\n        pass\n",
        encoding="utf-8",
    )
    import ast as _ast

    tree = _ast.parse(only_def.read_text())
    assert module._ast_call_sites_of("mark_persisted", tree) == []

    # File with a call expression — flagged.
    has_call = tmp_path / "has_call.py"
    has_call.write_text(
        "async def f(store):\n    await store.mark_persisted(123)\n",
        encoding="utf-8",
    )
    tree = _ast.parse(has_call.read_text())
    assert len(module._ast_call_sites_of("mark_persisted", tree)) == 1


def test_check_internal_ordering_catches_reversed_calls(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A reversed-order persist controller fires the ordering violation."""
    module = _load_script_module()

    reversed_pc = tmp_path / "bad_persist_controller.py"
    reversed_pc.write_text(
        "class C:\n"
        "    async def _migrate_one(self, chain_id):\n"
        "        await self._store.mark_persisted(chain_id)\n"
        "        await self._file.put(chain_id, {})\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "_PERSIST_CONTROLLER_REL", str(reversed_pc.name))
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)

    violations = module.check_persist_controller_internal_ordering()
    assert violations, "Expected ordering violation for mark_persisted-before-put"
    assert "ordering" in violations[0]


def test_check_internal_ordering_passes_when_put_first(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The reference (correct) order produces no violation."""
    module = _load_script_module()

    good_pc = tmp_path / "good_persist_controller.py"
    good_pc.write_text(
        "class C:\n"
        "    async def _migrate_one(self, chain_id):\n"
        "        await self._file.put(chain_id, {})\n"
        "        await self._store.mark_persisted(chain_id)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "_PERSIST_CONTROLLER_REL", str(good_pc.name))
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)

    violations = module.check_persist_controller_internal_ordering()
    assert violations == []
