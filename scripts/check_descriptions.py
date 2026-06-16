"""Validate every Pydantic BaseModel field has a non-empty description.

Discovers every package under src/*/src/, adds each package's src
directory to sys.path so the package's own internal imports resolve,
and imports every module under each package. Then iterates over every
BaseModel subclass and asserts each field's `description` is non-empty.

Exit codes:
- 0: every field is documented
- 1: one or more fields lack a non-empty description (violations listed)
- 2: one or more modules failed to import (failures listed; the run is
  inconclusive rather than a false clean exit)

Run via: uv run scripts/check_descriptions.py
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from pydantic import BaseModel


def _module_name_for(path: Path, package_root: Path, package_name: str) -> str:
    """Compute the dotted module name for a .py file inside a package root."""
    rel = path.relative_to(package_root)
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join([package_name, *parts]) if parts else package_name


def _all_models() -> tuple[list[type[BaseModel]], list[tuple[Path, Exception]]]:
    """Discover every BaseModel subclass under src/*/src/.

    Each src/<pkg>/src/ directory is added to sys.path so that the
    package's internal `from <pkg>.x import y` statements resolve. Each
    module is imported by its real dotted name (e.g. `phantom.config.probe`)
    so `dataclasses` and Pydantic can find the module in `sys.modules`.

    Discovered models are filtered to those whose `__module__` belongs to
    a workspace package — transitively-imported third-party models are
    not in scope of this check.

    Returns:
        (models, import_failures). Import failures are surfaced — not
        silently swallowed — so a broken import surfaces as exit 2
        instead of a false "all clean" exit 0.
    """
    root = Path(__file__).parent.parent
    models: set[type[BaseModel]] = set()
    failures: list[tuple[Path, Exception]] = []
    workspace_package_prefixes: set[str] = set()
    src_dirs = sorted(root.glob("src/*/src/"))
    for src_dir in src_dirs:
        for package_root in src_dir.iterdir():
            if package_root.is_dir() and (package_root / "__init__.py").is_file():
                workspace_package_prefixes.add(package_root.name)
    for src_dir in src_dirs:
        # Add the package's src directory to sys.path so the package's
        # own internal imports resolve when we import its modules below.
        sys.path.insert(0, str(src_dir))
        try:
            # Each immediate child of src_dir that is a package directory
            # corresponds to one importable top-level package.
            for package_root in sorted(src_dir.iterdir()):
                if not package_root.is_dir():
                    continue
                if not (package_root / "__init__.py").is_file():
                    continue
                package_name = package_root.name
                for path in sorted(package_root.rglob("*.py")):
                    module_name = _module_name_for(path, package_root, package_name)
                    try:
                        module = importlib.import_module(module_name)
                    except Exception as exc:
                        failures.append((path, exc))
                        continue
                    for obj in vars(module).values():
                        if (
                            isinstance(obj, type)
                            and issubclass(obj, BaseModel)
                            and obj is not BaseModel
                            and _belongs_to_workspace(obj, workspace_package_prefixes)
                        ):
                            models.add(obj)
        finally:
            sys.path.remove(str(src_dir))
    return list(models), failures


def _belongs_to_workspace(model: type[BaseModel], prefixes: set[str]) -> bool:
    """True if the model's defining module is inside a workspace package."""
    top_level = (model.__module__ or "").split(".", 1)[0]
    return top_level in prefixes


def main() -> int:
    """Entry point for the Pydantic field-description check."""
    models, failures = _all_models()
    if failures:
        print(f"Module-import failures ({len(failures)}):", file=sys.stderr)
        for path, exc in failures:
            print(f"  {path}: {exc!r}", file=sys.stderr)
        return 2
    violations: list[str] = []
    for model in models:
        for field_name, field_info in model.model_fields.items():
            description = field_info.description or ""
            if not description.strip():
                violations.append(
                    f"{model.__module__}.{model.__qualname__}.{field_name}",
                )
    if violations:
        print(f"Found {len(violations)} fields without description:", file=sys.stderr)
        for v in sorted(violations):
            print(f"  {v}", file=sys.stderr)
        return 1
    print("All Pydantic fields have non-empty descriptions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
