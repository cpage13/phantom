"""The ``conformance`` marker must mean what ADR-035 says it means (finding S12-1).

``E2E_SERVICE_CMD=<binary> pytest -m conformance`` is the trailing Go port's
acceptance gate (ADR-035, CONTEXT.md). The gate is only worth anything if every
test it collects actually runs the binary the seam names. Two marked modules did
not: one booted the Python service in-process through ``boot_stack``, the other
built the repo's Python Docker image and probed the container with
``python -c``. Both reported green for the Python reference while the binary
under test was never started, so a port that lost data would have passed.

ADR-035 states the rule this file mechanises: "The conformance marker's
classification rule is load-bearing: a test that pins Python-internal behavior
must stay unmarked, or the gate lies to the port. In-process-stack tests are
Python-implementation tests by construction."

Three static rules, each closing one way the gate can lie:

1. A conformance-marked test must not call the in-process stack entrypoint.
2. A conformance-marked test must not also be a ``docker``-lane test, because
   that lane builds and boots an image rather than the seam's command.
3. A module containing conformance-marked tests must reference the subprocess
   harness's launcher, which is the only thing that consults the seam.

This is a static AST sweep rather than a collection hook so it runs in the fast
contract lane, where a mis-marked test is caught on the PR that adds it rather
than on the next port acceptance run.
"""

from __future__ import annotations

import ast
from pathlib import Path

_E2E_ROOT: Path = Path(__file__).resolve().parents[1] / "e2e"

_CONFORMANCE_MARKER: str = "conformance"
_DOCKER_MARKER: str = "docker"
# The subprocess harness class whose argv resolution consults E2E_SERVICE_CMD.
# A module that never mentions it cannot be running the seam's binary.
_SEAM_LAUNCHER: str = "PhantomSubprocess"
# The in-process stack entrypoint. A test that calls it is running the Python
# service inside the pytest process, which is a Python-implementation test by
# construction (ADR-035).
_IN_PROCESS_STACK_ENTRYPOINT: str = "boot_stack"

# Floor on the number of conformance tests the sweep must find. Guards the
# guard: a walker that silently matched nothing would make all three rules below
# hold vacuously, which is the exact defect class this file exists to close. The
# suite carried 42 at the time of writing; the floor is deliberately well under
# that so ordinary curation does not trip it.
_MIN_CONFORMANCE_TESTS: int = 20


def _marker_name(node: ast.expr) -> str | None:
    """Return the marker name from a ``pytest.mark.<name>`` expression.

    Handles the bare attribute form and the called form
    (``pytest.mark.skipif(...)``). Anything else is not a marker
    expression and returns ``None``.
    """
    if isinstance(node, ast.Call):
        return _marker_name(node.func)
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
    ):
        return node.attr
    return None


def _module_marker_names(tree: ast.Module) -> set[str]:
    """Return marker names from a module-level ``pytestmark`` assignment."""
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if "pytestmark" not in targets:
            continue
        values = node.value.elts if isinstance(node.value, ast.List | ast.Tuple) else [node.value]
        for value in values:
            marker = _marker_name(value)
            if marker is not None:
                names.add(marker)
    return names


def _test_functions(tree: ast.Module) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    """Return every module-level function whose name starts with ``test_``."""
    return [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.name.startswith("test_")
    ]


def _decorator_marker_names(func: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """Return marker names from a test function's own decorators."""
    names: set[str] = set()
    for decorator in func.decorator_list:
        marker = _marker_name(decorator)
        if marker is not None:
            names.add(marker)
    return names


def _calls(func: ast.FunctionDef | ast.AsyncFunctionDef, name: str) -> bool:
    """Return True when the function body calls ``name`` directly."""
    for node in ast.walk(func):
        if isinstance(node, ast.Call):
            called = node.func
            if isinstance(called, ast.Name) and called.id == name:
                return True
            if isinstance(called, ast.Attribute) and called.attr == name:
                return True
    return False


def _references(tree: ast.Module, name: str) -> bool:
    """Return True when the module mentions ``name`` anywhere."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == name:
            return True
        if isinstance(node, ast.Attribute) and node.attr == name:
            return True
        if isinstance(node, ast.alias) and node.name == name:
            return True
    return False


def _conformance_tests() -> list[
    tuple[Path, ast.Module, ast.FunctionDef | ast.AsyncFunctionDef, set[str]]
]:
    """Return every conformance-marked e2e test with its module and markers.

    Returns:
        One tuple per marked test: ``(path, module tree, function node,
        effective marker names)``. Effective markers are the module-level
        ``pytestmark`` names unioned with the function's own decorators.
    """
    found: list[tuple[Path, ast.Module, ast.FunctionDef | ast.AsyncFunctionDef, set[str]]] = []
    for path in sorted(_E2E_ROOT.rglob("test_*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        module_markers = _module_marker_names(tree)
        for func in _test_functions(tree):
            markers = module_markers | _decorator_marker_names(func)
            if _CONFORMANCE_MARKER in markers:
                found.append((path, tree, func, markers))
    return found


def test_the_conformance_sweep_actually_finds_tests() -> None:
    """Objective: the three rules below are never checked against an empty set.

    Expected outcome: the AST walk finds at least ``_MIN_CONFORMANCE_TESTS``
    conformance-marked tests. Without this, a walker broken by a change in how
    markers are spelled would make every rule below pass by finding nothing,
    which is precisely the vacuous-gate failure this file guards against.
    """
    found = _conformance_tests()
    assert len(found) >= _MIN_CONFORMANCE_TESTS, (
        f"the conformance AST sweep found only {len(found)} marked tests under "
        f"{_E2E_ROOT}; the marker spelling or the walk is broken, and the rules "
        "below would hold vacuously"
    )


def test_conformance_tests_never_boot_the_in_process_stack() -> None:
    """Objective: a conformance test must run the binary, not the Python service in-process.

    Expected outcome: no conformance-marked test calls
    ``boot_stack``. ADR-035: in-process-stack tests are
    Python-implementation tests by construction, so marking one makes
    ``E2E_SERVICE_CMD=<binary> pytest -m conformance`` report green for the
    Python reference while never starting the binary.

    Falsifier: mark an in-process test ``conformance`` and this goes RED
    naming the test.
    """
    offenders = [
        f"{path.relative_to(_E2E_ROOT.parent)}::{func.name}"
        for path, _tree, func, _markers in _conformance_tests()
        if _calls(func, _IN_PROCESS_STACK_ENTRYPOINT)
    ]
    assert not offenders, (
        "conformance-marked tests that boot the Python service in-process via "
        f"{_IN_PROCESS_STACK_ENTRYPOINT}, so the E2E_SERVICE_CMD seam is never "
        f"consulted:\n  " + "\n  ".join(offenders)
    )


def test_conformance_tests_are_never_docker_lane_tests() -> None:
    """Objective: the docker lane builds an image, so it cannot honour the seam.

    Expected outcome: no test carries both ``conformance`` and ``docker``.
    A docker-lane test boots an image built from this repo's Dockerfiles
    rather than the command ``E2E_SERVICE_CMD`` names, so the binary under
    test is never started.

    Falsifier: add ``conformance`` back to a docker-lane module and this
    goes RED naming the test.
    """
    offenders = [
        f"{path.relative_to(_E2E_ROOT.parent)}::{func.name}"
        for path, _tree, func, markers in _conformance_tests()
        if _DOCKER_MARKER in markers
    ]
    assert not offenders, (
        "tests marked both conformance and docker; the docker lane boots an "
        "image built from this repo, not the seam's binary:\n  " + "\n  ".join(offenders)
    )


def test_modules_with_conformance_tests_reference_the_seam_launcher() -> None:
    """Objective: a conformance module must go through the harness that reads the seam.

    Expected outcome: every module holding a conformance-marked test
    references :class:`PhantomSubprocess`, the only launcher whose argv
    resolution consults ``E2E_SERVICE_CMD``.

    Falsifier: mark a test in a module that starts the service some other
    way and this goes RED naming the module.
    """
    offenders = sorted(
        {
            str(path.relative_to(_E2E_ROOT.parent))
            for path, tree, _func, _markers in _conformance_tests()
            if not _references(tree, _SEAM_LAUNCHER)
        }
    )
    assert not offenders, (
        f"modules with conformance-marked tests that never mention {_SEAM_LAUNCHER}, "
        "so nothing in them can be launching the seam's binary:\n  " + "\n  ".join(offenders)
    )
