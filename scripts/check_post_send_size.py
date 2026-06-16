"""Verify routes/send.py:post_send is under 100 lines.

The plan's Family 11 commits the route handler shape change. This
script parses routes/send.py via Python's ast module, locates the
post_send function, and counts the line span from its def to its
final statement. Exits 0 if <= 100 lines; exit 1 with a report
otherwise; exit 2 if post_send is not found.

Run via: uv run scripts/check_post_send_size.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

_MAX_LINES = 100


def main() -> int:
    """Entry point for the post_send size check."""
    repo_root = Path(__file__).parent.parent
    send_py = repo_root / "src" / "phantom-service" / "src" / "phantom" / "routes" / "send.py"
    tree = ast.parse(send_py.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "post_send":
            start = node.lineno
            end = node.end_lineno or start
            span = end - start + 1
            if span > _MAX_LINES:
                print(
                    f"post_send is {span} lines (max {_MAX_LINES}); "
                    f"shrink via routes/admission.py extraction.",
                    file=sys.stderr,
                )
                return 1
            print(f"post_send is {span} lines (within {_MAX_LINES}-line limit).")
            return 0
    print("post_send function not found in routes/send.py", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
