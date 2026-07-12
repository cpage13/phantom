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

When ``server.tls.enabled`` is set, that SAME single socket is served over
TLS (HTTPS) instead of plaintext — ``ssl_*`` kwargs are splatted into the
existing ``uvicorn.run`` (the cert pair comes from
:func:`phantom.runtime.tls_cert.resolve_tls_paths`); still ONE listener, not
a second server. TLS does not change the bind, only the wire.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from typing import TypedDict

logger = logging.getLogger(__name__)

# Default TCP port when ``bind_tcp`` carries no explicit ``:port`` suffix.
# Mirrors the ``ServerCfg.bind_tcp`` default ("127.0.0.1:8080").
_DEFAULT_TCP_PORT: int = 8080
# Default bind host when ``bind_tcp`` is an empty string (no host segment).
# Loopback, matching the same-machine-only deployment default.
_DEFAULT_TCP_HOST: str = "127.0.0.1"


class _SslKwargs(TypedDict, total=False):
    """The optional ``ssl_*`` keyword arguments for ``uvicorn.run``.

    A ``total=False`` TypedDict (not a bare ``dict[str, str]``) so mypy matches
    each key to ``uvicorn.run``'s correspondingly-named parameter — and so an
    EMPTY mapping (TLS off) splats to nothing, keeping the call byte-for-byte
    today's plaintext behavior. Populated only when ``server.tls.enabled``.
    """

    ssl_certfile: str
    ssl_keyfile: str
    ssl_keyfile_password: str


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

    # Uvicorn's lifespan implementation logs a post-start application
    # lifespan failure but does not stop its serving loop. Bridge the
    # composition-root TaskGroup to the production server explicitly: the
    # callback requests uvicorn's normal signal-driven shutdown. Pinned
    # uvicorn 0.46 restores its signal handlers after the graceful drain and
    # re-raises SIGTERM, so the measured process status is ``-SIGTERM``; this
    # callback does not return control to the final success return below.

    def _stop_after_worker_failure() -> None:
        """Request production-server shutdown for a supervised worker fault."""
        logger.critical("supervised worker failed; stopping Phantom process")
        os.kill(os.getpid(), signal.SIGTERM)

    # Pass the YAML path so the lifespan can install SIGHUP and the
    # ``POST /v1/admin/reload`` endpoint has a file to re-read.
    app = create_app(
        settings,
        settings_path=_Path(args.config),
        worker_failure_callback=_stop_after_worker_failure,
    )
    # One uvicorn server bound to the single listener. UDS takes precedence
    # over TCP (the documented connections-table posture): when
    # ``server.bind_uds`` is set, bind the Unix-domain socket; otherwise
    # partition ``server.bind_tcp`` into host / port. ``uvicorn.run`` owns
    # the process signals (clean SIGINT/SIGTERM drain); the lifespan installs
    # SIGHUP for hot reload (a distinct signal uvicorn does not touch).
    #
    # When ``server.tls.enabled``, ``ssl_kwargs`` (built below) is splatted
    # into the SAME call so the one socket serves HTTPS; empty otherwise
    # (byte-for-byte plaintext). NOT a second listener.
    tls = settings.server.tls
    ssl_kwargs: _SslKwargs = {}
    if tls.enabled:
        # Resolve to a usable (cert_path, key_path). For an operator-supplied
        # pair this validates existence; when both are None it auto-generates /
        # rotates a self-signed pair and returns the stable paths. (Import kept
        # local, like the other `__main__` runtime imports above.)
        from phantom.runtime.tls_cert import resolve_tls_paths

        cert_path, key_path = resolve_tls_paths(tls, settings.storage.data_dir)
        ssl_kwargs["ssl_certfile"] = cert_path
        ssl_kwargs["ssl_keyfile"] = key_path
        # ssl_keyfile_password applies ONLY to an OPERATOR-supplied encrypted
        # key. The TlsCfg XOR validator guarantees both-or-neither, so
        # `tls.cert_path is not None` IS exactly the operator-supplied case; the
        # `is None` case is auto-gen, whose key is written UNENCRYPTED — passing
        # a password there would make uvicorn try to decrypt an unencrypted key
        # and fail at bind. So gate the password on operator-supplied only.
        if tls.cert_path is not None and tls.key_password is not None:
            ssl_kwargs["ssl_keyfile_password"] = tls.key_password

    if settings.server.bind_uds is not None:
        uvicorn.run(app, uds=settings.server.bind_uds, **ssl_kwargs)
    else:
        host, _, port = settings.server.bind_tcp.partition(":")
        uvicorn.run(
            app,
            host=host or _DEFAULT_TCP_HOST,
            port=int(port or _DEFAULT_TCP_PORT),
            **ssl_kwargs,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
