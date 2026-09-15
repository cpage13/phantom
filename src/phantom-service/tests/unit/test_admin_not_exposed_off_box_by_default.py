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
  and EVERY declared loopback spelling emits none (S3-4 / S12-9: the
  boundary was tested with exactly one of the three spellings, and the
  untested ``::1`` member was unreachable through its caller's
  ``partition(":")`` parse, so an operator binding the IPv6 loopback got
  the exposure warning on every boot - a false alarm in front of the one
  warning that guards an unauthenticated destructive admin surface);
* :func:`host_is_loopback` itself answers for all three spellings;
* :func:`phantom.runtime.startup_checks.parse_bind_tcp` - the ONE parse
  both the warning and the launcher read - resolves every accepted
  spelling and refuses a malformed one at ``--validate`` time rather than
  at bind time;
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

import pytest
import yaml
from fastapi.routing import APIRoute
from phantom.__main__ import main
from phantom.app import create_app
from phantom.config.settings import (
    LOOPBACK_HOSTS,
    InstanceCfg,
    RouteCfg,
    ServerCfg,
    Settings,
    StorageCfg,
    host_is_loopback,
)
from phantom.runtime.startup_checks import (
    DEFAULT_TCP_HOST,
    DEFAULT_TCP_PORT,
    ConfigInvariantError,
    parse_bind_tcp,
)

logger = logging.getLogger(__name__)

# The same-machine-only default: the single listener binds loopback.
_LOOPBACK_BIND = "127.0.0.1:8080"
# A non-loopback bind an operator sets to expose the surface deliberately
# (the opt-in that must emit the unauthenticated-exposure warning).
_NON_LOOPBACK_BIND = "0.0.0.0:8080"
# Every spelling of the loopback bind an operator may legitimately write.
# The bracketed IPv6 form is the only one that can carry a port (RFC 3986
# § 3.2.2); the bare form is a host on its own. All three hosts are members
# of LOOPBACK_HOSTS, so none of them may trip the exposure warning.
_LOOPBACK_BINDS: tuple[str, ...] = (
    "127.0.0.1:8080",
    "[::1]:8080",
    "::1",
    "localhost:8080",
    "localhost",
)

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
    bind = parse_bind_tcp(server_cfg.bind_tcp)
    assert bind.host == "127.0.0.1", (
        f"the single listener must default to loopback (got {server_cfg.bind_tcp!r}); "
        "the loopback default bind is the admin access control (ADR-004)"
    )
    assert bind.port == 8080
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


@pytest.mark.parametrize("bind_tcp", _LOOPBACK_BINDS)
def test_every_loopback_spelling_emits_no_exposure_warning(tmp_path: Path, bind_tcp: str) -> None:
    """S3-4 / S12-9. All three declared loopback spellings are recognised.

    Objective: the trust boundary is ``LOOPBACK_HOSTS``, which declares
    ``127.0.0.1``, ``::1`` and ``localhost``. Every way an operator can
    legitimately spell a loopback bind - including the bracketed IPv6 form
    that carries a port, and the bare IPv6 form that cannot - must be
    recognised as loopback.

    Expected outcome: no exposure warning for any of them. With the old
    ``bind_tcp.partition(":")`` parse, ``[::1]:8080`` yielded the host
    ``"["`` and ``::1`` yielded ``""``, so neither matched
    ``LOOPBACK_HOSTS`` and the full "anyone who can reach this address"
    warning fired on every boot.
    """
    records = _attach_capture("phantom.app")
    create_app(_settings(tmp_path, bind_tcp=bind_tcp))
    warnings = "\n".join(r.getMessage() for r in records if r.levelno >= logging.WARNING)
    assert "unauthenticated" not in warnings.lower(), (
        f"{bind_tcp!r} is a loopback bind and must not trip the exposure warning: {warnings}"
    )


@pytest.mark.parametrize("host", sorted(LOOPBACK_HOSTS))
def test_host_is_loopback_answers_for_every_declared_spelling(host: str) -> None:
    """S12-9. ``host_is_loopback`` is tested directly, on all three members.

    Objective: the predicate that defines the trust boundary had no direct
    test at all - it was exercised only through one caller with one
    spelling. Expected outcome: every member of ``LOOPBACK_HOSTS`` is
    loopback, and a routable address is not.
    """
    assert host_is_loopback(host), f"{host!r} is a declared LOOPBACK_HOSTS member"
    assert not host_is_loopback("0.0.0.0"), "the wildcard bind is not loopback"


@pytest.mark.parametrize(
    ("bind_tcp", "expected_host", "expected_port"),
    [
        ("127.0.0.1:8080", "127.0.0.1", 8080),
        ("127.0.0.1", "127.0.0.1", DEFAULT_TCP_PORT),
        ("[::1]:9000", "::1", 9000),
        ("[::1]", "::1", DEFAULT_TCP_PORT),
        ("::1", "::1", DEFAULT_TCP_PORT),
        ("0.0.0.0:8080", "0.0.0.0", 8080),
        ("localhost:1", "localhost", 1),
        ("", DEFAULT_TCP_HOST, DEFAULT_TCP_PORT),
        (":9100", DEFAULT_TCP_HOST, 9100),
    ],
)
def test_parse_bind_tcp_resolves_every_accepted_spelling(
    bind_tcp: str, expected_host: str, expected_port: int
) -> None:
    """S3-4. The one bind parse handles IPv4, IPv6 and the omitted segments.

    Objective: the launcher and the loopback warning read ONE parse, so
    the address that is bound and the address the ADR-004 warning judges
    are the same value. Expected outcome: each spelling resolves to the
    host uvicorn should bind (brackets stripped) and the resolved port.
    """
    bind = parse_bind_tcp(bind_tcp)
    assert (bind.host, bind.port) == (expected_host, expected_port)


@pytest.mark.parametrize(
    "bind_tcp",
    ["[::1:8080", "[]:8080", "[::1]8080", "127.0.0.1:http", "127.0.0.1:70000"],
)
def test_parse_bind_tcp_refuses_a_malformed_address(bind_tcp: str) -> None:
    """S3-4. A malformed bind is a typed config error, not a launch traceback.

    Objective: ``bind_tcp`` is a free-form string that Pydantic cannot
    validate, so an unparseable value used to reach ``int()`` at launch
    and abort with a raw ``ValueError`` traceback - after ``--validate``
    had already passed the same config.

    Expected outcome: ``ConfigInvariantError``, the same class every other
    boot-time config invariant raises, which ``__main__`` turns into a
    "config validation failed" line and exit 1 at ``--validate`` time.
    """
    with pytest.raises(ConfigInvariantError, match=r"server\.bind_tcp"):
        parse_bind_tcp(bind_tcp)


def test_a_malformed_bind_warns_instead_of_crashing_create_app(tmp_path: Path) -> None:
    """S3-4. ``create_app`` still builds when the bind address is unparseable.

    Objective: the launcher refuses a malformed ``bind_tcp`` outright, but
    ``create_app`` is also reachable from embedders and tests that never go
    through it. Making the trust-boundary warning parse the address must
    not turn a bad string into an exception from the app factory.

    Expected outcome: the app is built, and the operator gets a warning
    that says the loopback boundary could NOT be confirmed - not a silent
    pass and not a crash.
    """
    records = _attach_capture("phantom.app")

    app = create_app(_settings(tmp_path, bind_tcp="[::1:8080"))

    assert app.title == "phantom"
    warnings = "\n".join(r.getMessage() for r in records if r.levelno >= logging.WARNING)
    assert "could not be parsed" in warnings, warnings
    assert "ADR-004" in warnings, warnings


def test_validate_refuses_a_malformed_bind_before_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """S3-4. ``--validate`` catches the bad bind the launcher would die on.

    Objective: ``--validate`` advertises itself as safe to run at deploy
    time and is the gate an operator's CI branches on, but it did not look
    at ``bind_tcp`` at all. A config with ``[::1]:8080`` passed it and then
    aborted at launch inside ``int(":1]:8080")``.

    Expected outcome: exit code 1 with "config validation failed" on
    stderr and an empty stdout - the same shape as every other
    ``--validate`` refusal.
    """
    cfg = tmp_path / "phantom.yaml"
    cfg.write_text(yaml.safe_dump({"server": {"bind_tcp": "[::1:8080"}}))
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg), "--validate"])

    exit_code = main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "config validation failed" in captured.err, captured.err
    assert "bind_tcp" in captured.err, captured.err
    assert captured.out == "", "a refused config must print no resolved settings"


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
