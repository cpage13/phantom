"""The ``PHANTOM_*`` environment overlay: what it may say, and what it may leak.

Two defects lived in ``config/settings.py``'s ``_apply_env_overlay`` and are
pinned here together because they are the same walk over ``os.environ``.

1. **The overlay logged every prefixed variable's LITERAL VALUE at INFO, and
   any prefixed variable that was not a settings field additionally failed the
   whole load with its value echoed into the ``SettingsError``** that
   ``__main__.py`` writes to stderr. The shipped example config tells operators
   to name their Entra ID client secret ``PHANTOM_UPSTREAM_CLIENT_SECRET``, so
   following the documented setup put a live secret into the log sink AND the
   crash text AND refused to boot. ``PHANTOM_SERVER__TLS__KEY_PASSWORD`` leaked
   through the same channel, and the shipped ``docker-compose.yml``'s
   ``PHANTOM_ORG`` / ``PHANTOM_TAG`` tripped the same boot refusal.

2. **Every non-string knob was unreachable through the environment.** The
   overlay injected raw ``str`` values into sub-models that all declare
   ``strict=True``, so ``PHANTOM_STORAGE__SQLITE__BUSY_TIMEOUT_MS=2000`` was
   ``Input should be a valid integer [type=int_type, input_value='2000']`` and
   the service would not start. The top-level ``strict=False`` reads as if it
   covered this but is inert for nested fields. Both env paths were affected:
   the overlay AND pydantic-settings' own source, which a bare ``Settings()``
   uses.

The tests run each case through the real ``load_settings`` and through a bare
``Settings()``, because the documented mechanism (``src/phantom-deploy``'s
README and the shipped compose file) reaches production through the first and
every direct construction through the second.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from phantom.config.settings import Settings, SettingsError, load_settings
from pydantic import ValidationError

# A value shaped like a credential, distinctive enough to grep out of any
# emitted text. Never asserted positively - only its ABSENCE is asserted.
_SECRET = "s3cret-Zx9-AAD-CLIENT-VALUE"

# The variable name the shipped example config tells operators to use for the
# Entra ID client secret (config/phantom.yaml.example, the ad_mint block).
_DOCUMENTED_SECRET_VAR = "PHANTOM_UPSTREAM_CLIENT_SECRET"

# The two variables the shipped src/phantom-deploy/docker-compose.yml reads
# from the operator's shell. Neither is a Phantom setting.
_COMPOSE_VARS = ("PHANTOM_ORG", "PHANTOM_TAG")


class _RecordingHandler(logging.Handler):
    """Root handler that keeps the formatted text of every record it sees."""

    def __init__(self) -> None:
        """Start with an empty transcript."""
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """Append the fully formatted record to the transcript."""
        self.lines.append(self.format(record))

    @property
    def text(self) -> str:
        """Every record emitted so far, joined."""
        return "\n".join(self.lines)


@pytest.fixture
def transcript() -> Iterator[_RecordingHandler]:
    """Capture every log record emitted during the test, at DEBUG.

    Resets the ``phantom`` logger's own level as well as the root's. Setting
    only the root is not enough and made this fixture order-dependent: a record
    is filtered at ITS OWN logger before it ever propagates, and
    :func:`phantom.observability.logging.configure_logging` pins ``phantom`` to
    the configured level. So any earlier test in the lane that configured
    logging left that logger at INFO, and this fixture's DEBUG root never saw
    the skip record at all. The test then passed alone and failed in the suite,
    which is the least useful failure mode a test can have.
    """
    handler = _RecordingHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    phantom_logger = logging.getLogger("phantom")
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved_phantom_level = phantom_logger.level
    root.handlers = [handler]
    root.setLevel(logging.DEBUG)
    phantom_logger.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        phantom_logger.setLevel(saved_phantom_level)


def _write_minimal_yaml(tmp_path: Path, **blocks: object) -> Path:
    """Write a YAML config carrying ``blocks`` and return its path."""
    path = tmp_path / "phantom.yaml"
    path.write_text(yaml.safe_dump(dict(blocks)), encoding="utf-8")
    return path


def test_a_documented_secret_env_var_neither_leaks_nor_blocks_the_boot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transcript: _RecordingHandler
) -> None:
    """The finding, in the exact shape the shipped docs produce.

    Objective: an operator who follows ``config/phantom.yaml.example`` and puts
    the Entra ID client secret in ``PHANTOM_UPSTREAM_CLIENT_SECRET`` must get a
    service that boots, with the secret absent from every emitted record and
    from any error text. The variable names no settings field, so the overlay
    ignores it - which is also what pydantic-settings' own source does with a
    variable that matches nothing.

    Success: ``load_settings`` returns, no emitted record contains the secret,
    and the variable's name IS mentioned (so the operator can see it was
    considered and skipped).
    """
    monkeypatch.setenv(_DOCUMENTED_SECRET_VAR, _SECRET)
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    settings = load_settings(path)

    assert settings.server.bind_tcp == "127.0.0.1:8080"
    assert _SECRET not in transcript.text, "the env var's VALUE reached a log record"
    assert _DOCUMENTED_SECRET_VAR in transcript.text, (
        "the variable's NAME should be reported so the skip is visible"
    )


def test_the_tls_passphrase_is_applied_without_its_value_being_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transcript: _RecordingHandler
) -> None:
    """A prefixed variable that IS a settings field must not leak either.

    Objective: ``server.tls.key_password`` is a real settings field, so it is
    applied rather than ignored - and the log line for an APPLIED override must
    still carry only the variable name and the field path it resolved to. This
    is the harder half of the rule: ignoring covers the unknown variables,
    while this covers the known secret-bearing one.

    Success: the override took effect, the log names the key and the resolved
    dotted path, and the passphrase itself appears nowhere.
    """
    monkeypatch.setenv("PHANTOM_SERVER__TLS__KEY_PASSWORD", _SECRET)
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    settings = load_settings(path)

    assert settings.server.tls.key_password is not None
    assert settings.server.tls.key_password.get_secret_value() == _SECRET
    assert _SECRET not in transcript.text, "the passphrase reached a log record"
    assert "server.tls.key_password" in transcript.text


def test_the_shipped_compose_variables_do_not_refuse_the_boot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phantom does not own the whole ``PHANTOM_`` namespace.

    Objective: ``src/phantom-deploy/docker-compose.yml`` reads ``PHANTOM_ORG``
    and ``PHANTOM_TAG`` from the operator's shell to build the image
    reference. Neither is a settings field, and injecting them hit
    ``extra="forbid"`` and turned an unrelated variable into a boot failure.

    Success: both are set and the load completes.
    """
    for name in _COMPOSE_VARS:
        monkeypatch.setenv(name, "acme")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    assert load_settings(path).server.bind_tcp == "127.0.0.1:8080"


@pytest.mark.parametrize(
    ("env_name", "raw", "reader", "expected"),
    [
        (
            "PHANTOM_STORAGE__SQLITE__BUSY_TIMEOUT_MS",
            "2000",
            lambda s: s.storage.sqlite.busy_timeout_ms,
            2000,
        ),
        (
            "PHANTOM_UPSTREAM__TIMEOUT_SECONDS",
            "600.5",
            lambda s: s.upstream.timeout_seconds,
            600.5,
        ),
        (
            "PHANTOM_OBSERVABILITY__LOG_TO_STDOUT",
            "false",
            lambda s: s.observability.log_to_stdout,
            False,
        ),
    ],
    ids=["int", "float", "bool"],
)
def test_a_scalar_env_override_coerces_on_both_env_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    raw: str,
    reader: object,
    expected: object,
) -> None:
    """The documented override mechanism must work for non-string knobs.

    Objective: the environment carries only strings and every sub-model
    declares ``strict=True``, so an int, float or bool knob set through
    ``PHANTOM_*`` failed the whole load with ``int_type`` / ``bool_type``
    rather than overriding anything. ``src/phantom-deploy/README.md``
    advertises the mechanism and the shipped compose file relies on it.

    Both env paths are exercised because both were dead: ``load_settings``'s
    overlay, and pydantic-settings' own source that a bare ``Settings()`` uses.

    Success: each of int, float and bool arrives as its declared type through
    both paths.
    """
    monkeypatch.setenv(env_name, raw)
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})
    get = reader  # narrow for readability at the call sites below

    assert get(load_settings(path)) == expected  # type: ignore[operator]
    assert get(Settings()) == expected  # type: ignore[operator]


def test_yaml_keeps_its_strict_typing(tmp_path: Path) -> None:
    """Coercion is an ENV-path affordance, not a loosening of the model.

    Objective: the environment has no types, so a string there must be
    coerced. A YAML file does have types, so a quoted ``"2000"`` where an
    integer belongs is the operator mistake it always was and must stay a loud
    error. Success: the same value that the environment may supply as a string
    is rejected when it comes from YAML.
    """
    path = _write_minimal_yaml(tmp_path, storage={"sqlite": {"busy_timeout_ms": "2000"}})

    with pytest.raises(SettingsError, match="busy_timeout_ms"):
        load_settings(path)


def test_an_uncoercible_env_value_is_still_a_loud_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ignoring unknown variables must not become ignoring bad values.

    Objective: the two rules are easy to confuse. A variable that names NO
    settings field is skipped; a variable that names one and carries a value
    the field cannot accept must still fail the load, naming the field, so a
    typo in a real knob is not silently dropped.

    Success: the load raises and the message names the field path.
    """
    monkeypatch.setenv("PHANTOM_STORAGE__SQLITE__BUSY_TIMEOUT_MS", "two-thousand")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    with pytest.raises(SettingsError, match="busy_timeout_ms"):
        load_settings(path)


def test_a_literal_and_a_nested_two_level_override_still_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The string-typed knobs the README advertises must not regress.

    Objective: the coercion pass resolves each variable against the model, so
    it is a new place a working override could break. The two examples in
    ``src/phantom-deploy/README.md`` are a ``Literal`` two levels deep and a
    ``Literal`` three levels deep; both stay strings and must still land.
    """
    monkeypatch.setenv("PHANTOM_STORAGE__BODY_STORE__MODE", "all_disk")
    monkeypatch.setenv("PHANTOM_STORAGE__SQLITE__SYNCHRONOUS", "FULL")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    settings = load_settings(path)

    assert settings.storage.body_store.mode == "all_disk"
    assert settings.storage.sqlite.synchronous == "FULL"


def test_an_unknown_value_for_a_known_literal_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Literal knob keeps its closed value set through the env path."""
    monkeypatch.setenv("PHANTOM_STORAGE__BODY_STORE__MODE", "all_tape")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    with pytest.raises(SettingsError, match="mode"):
        load_settings(path)


def test_reload_applies_the_same_env_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transcript: _RecordingHandler
) -> None:
    """SIGHUP and the admin reload run the same overlay, so they share the rules.

    Objective: ``Settings.reload_from_yaml`` calls the same overlay, so the
    leak re-emitted on EVERY reload and the boot refusal recurred there too.
    Success: a reload with both a documented-secret variable and a coerced
    scalar completes, applies the scalar, and leaks nothing.
    """
    monkeypatch.setenv(_DOCUMENTED_SECRET_VAR, _SECRET)
    monkeypatch.setenv("PHANTOM_STORAGE__SQLITE__BUSY_TIMEOUT_MS", "2500")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    reloaded = Settings.reload_from_yaml(path)

    assert reloaded.storage.sqlite.busy_timeout_ms == 2500
    assert _SECRET not in transcript.text


def test_an_applied_override_is_reported_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transcript: _RecordingHandler
) -> None:
    """One load, one line per applied override.

    Objective: the overlay is now also the model's env settings source, so it
    runs twice per ``load_settings`` (once to build the init kwargs, once as
    the source). Reporting from both would double every operator-facing line.
    Success: exactly one "Applied env override" line for one variable.
    """
    monkeypatch.setenv("PHANTOM_SERVER__BIND_TCP", "0.0.0.0:9090")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    settings = load_settings(path)

    assert settings.server.bind_tcp == "0.0.0.0:9090"
    applied = [line for line in transcript.lines if "Applied env override" in line]
    assert len(applied) == 1, applied


def test_a_bad_settings_value_error_never_carries_a_prefixed_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The crash text is the second sink, and it must be clean too.

    Objective: ``__main__.py`` writes the ``SettingsError`` to stderr, so a
    validation failure anywhere in the config used to publish every prefixed
    variable's value into CI logs and deploy transcripts. Success: with a
    documented-secret variable set AND an unrelated invalid knob, the raised
    error names the bad knob and does not contain the secret.
    """
    monkeypatch.setenv(_DOCUMENTED_SECRET_VAR, _SECRET)
    monkeypatch.setenv("PHANTOM_SERVER__TLS__KEY_PASSWORD", _SECRET)
    path = _write_minimal_yaml(tmp_path, retention={"reaper_interval_seconds": 0})

    with pytest.raises(SettingsError) as caught:
        load_settings(path)

    message = str(caught.value)
    assert "reaper_interval_seconds" in message
    assert _SECRET not in message


def test_a_bare_settings_construction_ignores_unknown_prefixed_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The model's own env source obeys the same ignore rule.

    Objective: ``Settings()`` is constructed directly in tests and by any
    caller that does not go through ``load_settings``; it must not diverge
    from it. Success: an unknown prefixed variable does not raise.
    """
    monkeypatch.setenv(_DOCUMENTED_SECRET_VAR, _SECRET)

    assert Settings().instances == []


def test_instances_is_still_not_env_overlay_able(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The list-field refusal is a separate rule and must survive.

    Objective: ``instances`` IS a settings field, so the ignore rule does not
    cover it; the explicit refusal in ``load_settings`` is what keeps a
    half-parsed instance list out of the model. Success: still a
    ``SettingsError``.
    """
    monkeypatch.setenv("PHANTOM_INSTANCES", "garbage")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    with pytest.raises(SettingsError, match="PHANTOM_INSTANCES"):
        load_settings(path)


def test_a_deep_sub_block_override_resolves_and_coerces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Path resolution must reach a nested sub-block, and only then coerce.

    Objective: the overlay walks the model to find a variable's target field,
    and the ignore rule means a walk that stops early no longer fails loudly -
    it silently drops a real override. ``server.tls.enabled`` is three levels
    down and is a bool, so it exercises the walk and the coercion together.

    Success: the deep bool override lands, while the same string handed
    straight to the strict sub-model is still rejected - which is what makes
    the env path's coercion load-bearing rather than incidental.
    """
    monkeypatch.setenv("PHANTOM_SERVER__TLS__ENABLED", "true")
    path = _write_minimal_yaml(tmp_path, server={"bind_tcp": "127.0.0.1:8080"})

    assert load_settings(path).server.tls.enabled is True

    monkeypatch.delenv("PHANTOM_SERVER__TLS__ENABLED")
    with pytest.raises(ValidationError):
        Settings(server={"tls": {"enabled": "true"}})  # type: ignore[arg-type]
