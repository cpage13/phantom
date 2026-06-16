#!/usr/bin/env bash
# Pre-commit hook: run `mypy --strict -p <package>` per Python package.
#
# Workspace-level `mypy --strict src/` collides on dual-named modules
# (per-package `tests/` __init__.py files, multiple `src/<pkg>/src/`
# layouts). Each package's own venv has its third-party stubs (httpx,
# fastapi, uvicorn), so per-package invocation is the working pattern.
#
# Uses a plain indexed array of "<dir>:<module>" entries (not a bash-4
# `declare -A` associative array) so the hook also runs under macOS's
# system bash 3.2 — `declare -A` aborts there with `src: unbound variable`,
# failing the hook locally even though mypy itself is clean.

set -euo pipefail

# Each entry is "<package_dir>:<import_module>". Module names never contain
# ':' so `%%:*` / `#*:` split unambiguously.
packages=(
  "src/phantom-service:phantom"
  "src/phantom-client:phantom_client"
  "src/phantom-emulator:phantom_emulator"
)

failed=0
for entry in "${packages[@]}"; do
  pkg_dir="${entry%%:*}"
  module="${entry#*:}"
  if [ ! -d "$pkg_dir" ]; then
    continue
  fi
  echo "[mypy] $pkg_dir -> -p $module"
  if ! (cd "$pkg_dir" && uv run mypy --strict -p "$module"); then
    failed=1
  fi
done

exit $failed
