"""An unreadable settings file must fail the reload inside the contract (R8-2).

ADR-013's failure contract for hot reload: "Parse failures
(``yaml.YAMLError``) and validation failures (``pydantic.ValidationError``)
surface as 422 on the admin endpoint and as a logged warning on SIGHUP -
the in-flight ``SettingsHolder`` is not swapped." ADR-017 then promises
"Every Phantom error response (2xx admin replies excepted) carries a
JSON body" of the ``ErrorEnvelope`` shape, and documents a 500
``internal_error`` row as the "catch-all for unexpected exceptions
reaching the FastAPI error handler".

The implemented failure set is narrower than the real one.
``Settings.reload_from_yaml`` starts with ``path.read_text(encoding="utf-8")``,
which raises:

* ``UnicodeDecodeError`` when the YAML carries a byte that is not valid
  UTF-8 - one pasted latin-1 character in a comment is enough, the
  classic hand-edited-config corruption on a producer, and
* ``FileNotFoundError`` when the file is missing at reload time (an
  operator mv, or a non-atomic editor save window).

Neither is ``yaml.YAMLError`` nor ``ValidationError``, and no
``internal_error`` catch-all handler is registered anywhere
(``register_admin_error_handlers`` registers ten typed handlers; ADR-017's
documented catch-all row is unimplemented), so:

* ``POST /v1/admin/reload`` answers Starlette's raw ``text/plain``
  500 "Internal Server Error" - no envelope, no ``error.code`` to
  dispatch on (the same ADR-017 escape class as R6-4 and R7-1's route
  leg), and
* the SIGHUP path (``_sighup_reload`` documents "a parse or validation
  error must not crash the process. Log and keep the previous
  snapshot.") lets the exception escape the reload task: no documented
  warning is logged, and the failure surfaces only as an asyncio
  unretrieved-task-exception report instead of the contract's
  swallow-and-log.

The running config is NOT half-applied (the read fails before any
swap), which is why this is the LOW sibling of R7-1, not its repeat: the
defect is the failure-envelope discipline, not atomicity. All three
tests pin the operator-observable contract; the natural fix routes the
read failures through the documented reject path (or implements the
documented ``internal_error`` catch-all), either of which flips them.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.settings_holder import SettingsHolder
from phantom.models.errors import ErrorEnvelope
from phantom.routes import admin as admin_routes
from phantom.runtime.reload import _sighup_reload

# A YAML payload whose only flaw is one latin-1 byte (0xE9, "e acute")
# in a trailing comment - invalid as UTF-8, so the decode fails before
# the YAML parser ever runs. Structurally the document is valid.
_NON_UTF8_YAML_BYTES: bytes = b"storage:\n  data_dir: /tmp/phantom-data  # caf\xe9\n"

# R8-2 (fixed): RELOAD_FAILURE_ERRORS is the one shared failure set;
# read failures (vanished file, non-UTF-8 byte) now reject-and-keep,
# enveloped on the route and logged-and-swallowed on SIGHUP.


def _reload_capable_admin_app(settings_path: Path) -> FastAPI:
    """Mount the admin router wired for reload exactly as production does.

    ``create_app`` exposes the reload surface via three ``app.state``
    attributes (``settings_holder`` / ``settings_path`` / ``instances``)
    plus the one shared error-handler registration helper; this mirrors
    that wiring with an empty instance list (the failure under test
    fires in the YAML read, before any instance is touched).
    """
    app = FastAPI()
    app.include_router(admin_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.state.settings_holder = SettingsHolder({})
    app.state.settings_path = settings_path
    app.state.instances = []
    return app


def test_reload_route_answers_in_envelope_when_settings_file_is_not_utf8(
    tmp_path: Path,
) -> None:
    """POST /v1/admin/reload on a non-UTF-8 file must ride the envelope.

    Attack: corrupt the live YAML with a single latin-1 byte (a pasted
    comment character - the realistic hand-edit slip) and hit the
    reload route. ADR-017 promises every error response carries an
    ``ErrorEnvelope`` with a dispatchable ``error.code``; an operator
    tool dispatching on the code must learn "the reload failed, fix the
    file" rather than falling through on a raw ``text/plain`` 500.
    """
    bad_yaml = tmp_path / "phantom.yaml"
    bad_yaml.write_bytes(_NON_UTF8_YAML_BYTES)
    client = TestClient(_reload_capable_admin_app(bad_yaml), raise_server_exceptions=False)

    response = client.post("/v1/admin/reload")

    envelope = ErrorEnvelope.model_validate(response.json())
    assert envelope.error.code, (
        "the reload failure must carry a dispatchable error.code per "
        f"ADR-017; got status {response.status_code} body {response.text!r}"
    )


async def test_sighup_reload_swallows_a_non_utf8_settings_file(tmp_path: Path) -> None:
    """The SIGHUP reload path must log-and-keep on a non-UTF-8 file.

    ``_sighup_reload``'s contract (its own docstring, mirroring
    ADR-013): a bad settings file "must not crash the process. Log and
    keep the previous snapshot." A ``UnicodeDecodeError`` instead
    escapes the reload task - the operator gets no documented warning,
    only an asyncio unretrieved-task-exception report, and learns the
    reload never applied much later, if at all.
    """
    bad_yaml = tmp_path / "phantom.yaml"
    bad_yaml.write_bytes(_NON_UTF8_YAML_BYTES)

    await _sighup_reload(SettingsHolder({}), bad_yaml, [])


async def test_sighup_reload_swallows_a_vanished_settings_file(tmp_path: Path) -> None:
    """The SIGHUP reload path must log-and-keep when the file is missing.

    Same contract as the non-UTF-8 leg for the other realistic read
    failure: the YAML path no longer exists at reload time (operator
    mv/rename, or a non-atomic editor save window). Today the
    ``FileNotFoundError`` escapes the reload task instead of the
    documented log-and-keep-previous.
    """
    vanished = tmp_path / "phantom.yaml"

    await _sighup_reload(SettingsHolder({}), vanished, [])
