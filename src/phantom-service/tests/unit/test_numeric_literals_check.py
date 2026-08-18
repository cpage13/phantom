"""Unit test for the pre-commit ``check_numeric_literals.py`` script.

CL11, and objective M5. The gate this replaces could not fail: an
unconditional ``exit 0``, a regex blind to keyword arguments, no ``scripts/``
coverage, and an invalid BSD-grep character range that sent its own grep's
exit-2 into ``/dev/null``. So the deliverable is not the checker, it is the
proof that the checker CAN fail on a seeded violation and does not on the
tree it ships against.

Three tests, one per thing that can go wrong:

1. the production tree passes, so the gate can be hard rather than advisory;
2. a planted ``foo(timeout=45)`` is flagged, naming its file and line, which
   is the M5 seeded violation and the half the old hook could never do;
3. every exemption is exercised, so a future tightening cannot quietly
   swallow the rule by widening an exemption until nothing is left.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_numeric_literals.py"


def _load_script_module() -> ModuleType:
    """Load the script as a module so its functions are unit-testable."""
    spec = importlib.util.spec_from_file_location("check_numeric_literals", _SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_numeric_literals"] = module
    spec.loader.exec_module(module)
    return module


def test_the_production_tree_passes() -> None:
    """The whole configured scope is clean, which is what lets the gate be hard.

    Objective: every bare numeric literal the rule flags has been bound to a
    named constant or moved to configuration. Success is an empty violation
    list over the default scope, ``src/*/src/`` plus ``scripts/``.

    A gate that fires on the tree it ships with is one a developer learns to
    bypass, so this assertion is the precondition for the hook being a hard
    exit rather than an informational print.
    """
    module = _load_script_module()
    violations = module.find_violations()
    assert violations == [], (
        f"the numeric-literal gate must pass on the production tree; violations: {violations}"
    )


def test_a_planted_keyword_literal_is_flagged(tmp_path: Path) -> None:
    """A seeded ``foo(timeout=45)`` produces exactly one hit naming file and line.

    Objective (M5): the gate can FAIL. Success is one violation whose message
    carries the planted file's name and the line the literal sits on.

    This is the assertion the replaced hook could not make in any form: its
    regex matched only indented whole-line assignments, so a policy number
    passed as a keyword argument at a call site, which is exactly where such
    numbers hide, was invisible to it.
    """
    module = _load_script_module()
    planted = tmp_path / "planted.py"
    planted.write_text("def call():\n    return foo(timeout=45)\n", encoding="utf-8")

    violations = module.find_violations([planted])

    assert len(violations) == 1, violations
    assert "planted.py" in violations[0]
    assert ":2:" in violations[0]
    assert "timeout=45" in violations[0]


def test_every_exemption_returns_no_violation(tmp_path: Path) -> None:
    """One case per exemption, so a future tightening cannot swallow the rule.

    Objective: the four exemptions are each real and each narrow. Success is
    zero violations for a file exercising all of them together, and it is
    written as one file per exemption so a failure names which one broke.

    The exemptions, in the order the script declares them: the structural
    values 0 and 1; a literal inside a ``Field``-family call, whose meaning is
    carried by the adjacent required ``description``; a ``status`` or
    ``status_code`` keyword, where the number IS its own name; and a
    module-level UPPER_CASE constant, which is the convention's prescribed
    form rather than a violation of it.

    Default parameter values are exercised here too, because dropping that
    leg was round 2's decision and a silent re-introduction would flag nine
    sites this phase does not own.
    """
    module = _load_script_module()
    cases = {
        "structural": "def f(items):\n    return items[0] + 1\n",
        "described_field": (
            "from pydantic import BaseModel, Field\n\n\n"
            "class M(BaseModel):\n"
            '    port: int = Field(8080, ge=1024, le=65535, description="the bind port")\n'
        ),
        "self_naming_status": "def f(r):\n    return r.respond(status_code=404)\n",
        "module_constant": (
            "_TIMEOUT_SECONDS = 45\n\n\ndef f(c):\n    return c(x=_TIMEOUT_SECONDS)\n"
        ),
        "default_parameter": (
            "def f(level: int = 3, shard: int = 2) -> int:\n    return level + shard\n"
        ),
    }
    for name, source in cases.items():
        path = tmp_path / f"{name}.py"
        path.write_text(source, encoding="utf-8")
        assert module.find_violations([path]) == [], f"exemption {name!r} no longer holds"
