"""The admin surface is not exposed off-box by default (the loopback bind).

R12-1 found that the destructive admin endpoints (``DELETE
/v1/admin/chains`` bulk delete, ``DELETE /v1/admin/chains/{chain_id}``,
``DELETE /v1/admin/tokens``, ``POST /v1/admin/reload``) were reachable
UNAUTHENTICATED on every public interface. A two-listener split (bind the
admin router on its own loopback socket) was tried as the fix and then
collapsed: the deployment is same-machine-only (Phantom runs on the SAME
box as its producer and is reached over loopback), so the split provided no
benefit and introduced two bugs (R13-1 startup-ordering, R13-2
bind-collision). The single listener eliminates both by construction.

The protected property is UNCHANGED in substance - "admin is not exposed
off-box by default" - but the mechanism is now the LOOPBACK BIND, not a
port split. :func:`phantom.app.create_app` returns ONE ``FastAPI`` app
serving intake + admin + health on one socket; ``server.bind_tcp`` defaults
to ``127.0.0.1:8080`` (loopback), so admin (like everything) is reachable
only on the machine. That loopback bind IS the admin access control
(ADR-004); an operator who wants network reachability sets ``bind_tcp``
explicitly (e.g. ``0.0.0.0:8080``) and gets the unauthenticated-exposure
warning.

This module pins, over the REAL ``create_app``:

* the default bind is loopback (so the admin surface is not reachable
  off-box by default) - the property the collapse preserves;
* a non-loopback ``bind_tcp`` emits the unauthenticated-exposure warning,
  and the loopback default emits none;
* the ONE app serves intake (``POST /v1/send``), the destructive admin
  route (``DELETE /v1/admin/chains``), and the public liveness/readiness
  probes (``GET /v1/healthz`` / ``GET /v1/readyz``) - the collapse to one
  app is intentional, and the loopback bind (not a port split) is the
  control;
* the worker pool starts EXACTLY ONCE in the single app's lifespan.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi.routing import APIRoute
from phantom.app import create_app
from phantom.config.settings import (
    InstanceCfg,
    RouteCfg,
    ServerCfg,
    Settings,
    StorageCfg,
)

logger = logging.getLogger(__name__)

# The same-machine-only default: the single listener binds loopback.
_LOOPBACK_BIND = "127.0.0.1:8080"
# A non-loopback bind an operator sets to expose the surface deliberately
# (the opt-in that must emit the unauthenticated-exposure warning).
_NON_LOOPBACK_BIND = "0.0.0.0:8080"

# The most destructive admin route: bulk delete by filter. An
# off-box caller invoking this would destroy accepted, not-yet-delivered
# uploads (north-star data loss) - which the loopback default bind prevents.
_DESTRUCTIVE_ADMIN_PATH = "/v1/admin/chains"
_DESTRUCTIVE_ADMIN_METHOD = "DELETE"

# The intake route that proves "this is the producer-facing app": anonymous
# chain submission.
_INTAKE_PATH = "/v1/send"

# The public liveness + readiness probes (kept on the single app so a
# container/orchestrator probe reaches them on the one listener).
_LIVENESS_PATH = "/v1/healthz"
_READINESS_PATH = "/v1/readyz"


def _settings(data_root: Path, *, bind_tcp: str = _LOOPBACK_BIND) -> Settings:
    """Production-shaped Settings with the given ``bind_tcp``.

    Args:
        data_root: Temp directory for the instance storage tree.
        bind_tcp: The single listener's TCP bind (loopback by default).

    Returns:
        A valid :class:`Settings` with one single-route instance.
    """
    hosts = ["files.example.com"]
    return Settings(
        server=ServerCfg(bind_tcp=bind_tcp),
        storage=StorageCfg(data_dir=str(data_root)),
        instances=[
            InstanceCfg(
                id="primary",
                host_prefixes=hosts,
                data_dir="primary",
                routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
            )
        ],
    )


def _app_serves(app: object, *, path: str, method: str) -> bool:
    """Return whether ``app`` has a mounted route matching ``path`` + ``method``.

    Inspects the FastAPI route table directly (no lifespan entry, no
    workers): a route is "served by this application" iff it appears in
    ``app.routes`` with the given path and HTTP method.

    Args:
        app: The FastAPI application to inspect.
        path: The exact route path (e.g. ``/v1/admin/chains``).
        method: The HTTP method (e.g. ``DELETE``).

    Returns:
        ``True`` if the application would route ``method path`` to a handler.
    """
    routes = getattr(app, "routes", [])
    for route in routes:
        if isinstance(route, APIRoute) and route.path == path and method in route.methods:
            return True
    return False


def test_default_bind_is_loopback() -> None:
    """The single listener defaults to loopback (admin not exposed off-box).

    THE LOAD-BEARING PROPERTY: with no operator override, the one listener
    binds ``127.0.0.1`` - so the admin surface (which rides this listener)
    is reachable only on the machine. The loopback bind is the admin access
    control (ADR-004); this is how R12-1 stays fixed in the one-listener
    world.
    """
    server_cfg = ServerCfg()
    host, _, port = server_cfg.bind_tcp.partition(":")
    assert host == "127.0.0.1", (
        f"the single listener must default to loopback (got {server_cfg.bind_tcp!r}); "
        "the loopback default bind is the admin access control (ADR-004)"
    )
    assert port == "8080"
    assert server_cfg.bind_uds is None


def test_one_app_serves_intake_admin_and_health(tmp_path: Path) -> None:
    """The single app serves intake + the destructive admin route + health.

    The collapse to one app is intentional (the loopback bind is the
    control, not a port split). The ONE app must serve anonymous intake
    (``POST /v1/send``), the destructive admin route (``DELETE
    /v1/admin/chains``), and the public liveness/readiness probes - all on
    the one loopback-bound socket.
    """
    app = create_app(_settings(tmp_path))
    assert _app_serves(app, path=_INTAKE_PATH, method="POST"), (
        f"the single app must serve intake {_INTAKE_PATH}"
    )
    assert _app_serves(app, path=_DESTRUCTIVE_ADMIN_PATH, method=_DESTRUCTIVE_ADMIN_METHOD), (
        f"the single app must serve the admin route {_DESTRUCTIVE_ADMIN_METHOD} "
        f"{_DESTRUCTIVE_ADMIN_PATH} (it rides the same loopback listener as intake)"
    )
    assert _app_serves(app, path=_LIVENESS_PATH, method="GET"), (
        f"the single app must serve liveness {_LIVENESS_PATH}"
    )
    assert _app_serves(app, path=_READINESS_PATH, method="GET"), (
        f"the single app must serve readiness {_READINESS_PATH}"
    )


def _attach_capture(logger_name: str) -> list[logging.LogRecord]:
    """Attach a record-capturing handler to ``logger_name`` and return the list.

    ``create_app`` calls ``configure_logging`` which does
    ``root.handlers.clear()``, so pytest's ``caplog`` root handler is removed
    before ``create_app`` logs. Attaching directly to the named logger (which
    is not cleared) captures its records reliably - the same pattern
    ``test_startup_guards_prod_path.py`` uses.

    Args:
        logger_name: The dotted logger name to capture (``"phantom.app"``).

    Returns:
        A list that accrues every record the named logger emits.
    """
    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    captured_logger = logging.getLogger(logger_name)
    captured_logger.addHandler(_ListHandler())
    captured_logger.setLevel(logging.WARNING)
    return records


def test_non_loopback_bind_emits_unauthenticated_warning(tmp_path: Path) -> None:
    """A non-loopback ``bind_tcp`` warns at startup that admin is unauthenticated.

    The remote opt-in is allowed (the server keeps serving) but a prominent
    ``logger.warning`` names the host, states the admin endpoints are
    UNAUTHENTICATED (they ride this same listener), instructs an
    authenticating reverse proxy, and cites ADR-004.
    """
    records = _attach_capture("phantom.app")
    app = create_app(_settings(tmp_path, bind_tcp=_NON_LOOPBACK_BIND))
    assert app.title == "phantom"
    warnings = [r.getMessage() for r in records if r.levelno >= logging.WARNING]
    joined = "\n".join(warnings)
    assert "0.0.0.0" in joined, joined
    assert "unauthenticated" in joined.lower(), joined
    assert "ADR-004" in joined, joined


def test_loopback_bind_emits_no_unauthenticated_warning(tmp_path: Path) -> None:
    """The same-machine-only loopback default does NOT emit the exposure warning."""
    records = _attach_capture("phantom.app")
    create_app(_settings(tmp_path))
    warnings = "\n".join(r.getMessage() for r in records if r.levelno >= logging.WARNING)
    assert "unauthenticated" not in warnings.lower(), warnings


async def test_workers_start_exactly_once_in_the_single_lifespan(tmp_path: Path) -> None:
    """The single app's lifespan builds the configured instance EXACTLY ONCE.

    Entering the app's lifespan opens the one instance store, runs recovery,
    and spawns the worker TaskGroup. There is exactly one app with exactly
    one lifespan, so a double-start across listeners is structurally
    impossible (there is only one listener).
    """
    app = create_app(_settings(tmp_path))
    # No instance is built at construction (only inside the lifespan).
    assert app.state.instances == [], "the app must not have built any instance at construction"
    async with app.router.lifespan_context(app):
        assert [inst.cfg.id for inst in app.state.instances] == ["primary"], (
            "the lifespan must build the configured instance exactly once"
        )
