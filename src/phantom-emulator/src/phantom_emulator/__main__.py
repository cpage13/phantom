"""Command-line entry point for ``python -m phantom_emulator``.

This is the launcher used by the Docker container. For in-process
tests, :func:`phantom_emulator.server.start_server` is the right
entry point - it gives back a typed handle and avoids the global
event loop.
"""

from __future__ import annotations

import argparse
import logging

import uvicorn

from phantom_emulator.app import create_app
from phantom_emulator.config import load_config

logger = logging.getLogger(__name__)


def main() -> None:
    """Parse CLI args, load config, and start uvicorn."""
    parser = argparse.ArgumentParser(prog="phantom-emulator")
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="Optional YAML config path.",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    logging.basicConfig(level=cfg.logging.level)

    app = create_app(cfg)
    uvicorn.run(
        app,
        host=cfg.server.host,
        port=cfg.server.port,
        log_level=cfg.logging.level.lower(),
    )


if __name__ == "__main__":
    main()
