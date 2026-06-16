"""``python -m phantom`` entry point — uvicorn launcher (and ``--validate``).

A Phantom process serves ONE ASGI application on ONE socket: intake
(``POST /v1/send``), the admin surface (``/v1/admin/*``), and the public
liveness/readiness probes all ride the single listener. The deployment is
same-machine-only (Phantom runs on the SAME box as its producer, reached over
loopback), so the listener binds ``server.bind_tcp`` (default
``127.0.0.1:8080`` - loopback) or ``server.bind_uds`` when set. That
loopback default bind IS the admin access control (ADR-004): admin, like
everything, is reachable only on the machine. An operator who wants network
reachability sets ``bind_tcp`` explicitly (e.g. ``0.0.0.0:8080``) and gets
the unauthenticated-exposure warning from :func:`phantom.app.create_app`.
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)

# Default TCP port when ``bind_tcp`` carries no explicit ``:port`` suffix.
# Mirrors the ``ServerCfg.bind_tcp`` default ("127.0.0.1:8080").
_DEFAULT_TCP_PORT: int = 8080
# Default bind host when ``bind_tcp`` is an empty string (no host segment).
# Loopback, matching the same-machine-only deployment default.
_DEFAULT_TCP_HOST: str = "127.0.0.1"


def main() -> int:
    """Parse args and either validate the config or run uvicorn.

    Returns:
        Process exit code (0 on success). Returned (not just used with
        ``sys.exit``) so the ``__name__ == '__main__'`` branch can exit
        cleanly without conflating "validate-error" with "uvicorn returned".
    """
    parser = argparse.ArgumentParser(prog="phantom")
    parser.add_argument("-c", "--config", required=True, help="Path to phantom.yaml")
    parser.add_argument(
        "--validate",
        action="store_true",
        help=(
            "Load the config, run all Pydantic validators, print the "
            "resolved settings (or the first validation error), and exit "
            "0/1. Does NOT bind a server — safe to run at deploy time."
        ),
    )
    args = parser.parse_args()

    # Imports kept inside `main` so `python -m phantom --help` doesn't load
    # the full FastAPI dep tree just to print usage.
    from phantom.config.settings import SettingsError, load_settings

    try:
        settings = load_settings(args.config)
    except SettingsError as exc:
        # Validation failure: print on stderr; exit 1. The operator running
        # `--validate` in CI gets a non-zero exit they can branch on.
        sys.stderr.write(f"config validation failed: {exc}\n")
        return 1

    if args.validate:
        # `model_dump_json(indent=2)` is a load-bearing print: the operator
        # WANTS to see the resolved settings (post-env-overlay) so they
        # can confirm "yes this is what production will see." Per project
        # rules (`logging.getLogger` instead of `print` for operational
        # output) — but `--validate` is a one-shot CLI tool, not operational
        # output. `sys.stdout.write` keeps the contract: stdout = the answer.
        sys.stdout.write(settings.model_dump_json(indent=2) + "\n")
        return 0

    from pathlib import Path as _Path

    import uvicorn

    from phantom.app import create_app

    # Pass the YAML path so the lifespan can install SIGHUP and the
    # ``POST /v1/admin/reload`` endpoint has a file to re-read.
    app = create_app(settings, settings_path=_Path(args.config))
    # One uvicorn server bound to the single listener. UDS takes precedence
    # over TCP (the documented connections-table posture): when
    # ``server.bind_uds`` is set, bind the Unix-domain socket; otherwise
    # partition ``server.bind_tcp`` into host / port. ``uvicorn.run`` owns
    # the process signals (clean SIGINT/SIGTERM drain); the lifespan installs
    # SIGHUP for hot reload (a distinct signal uvicorn does not touch).
    if settings.server.bind_uds is not None:
        uvicorn.run(app, uds=settings.server.bind_uds)
    else:
        host, _, port = settings.server.bind_tcp.partition(":")
        uvicorn.run(app, host=host or _DEFAULT_TCP_HOST, port=int(port or _DEFAULT_TCP_PORT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
