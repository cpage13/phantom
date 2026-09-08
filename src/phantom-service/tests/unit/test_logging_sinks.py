"""F15: the two documented logging sink knobs are real.

``ObservabilityCfg`` declared ``log_to_stdout`` (default ``True``,
described as "Stream log records to stdout") and ``log_to_file``
("Optional path of a secondary log-file sink"). ``configure_logging``
took only the level and always installed one bare
``logging.StreamHandler()``, which defaults to STDERR, so the shipped
default was contradicted on every deployment and the file path validated,
exported into ``contracts/settings.schema.json``, appeared in the example
config, and did nothing.

Every test here saves and restores the root logger's handlers and level,
because ``configure_logging`` clears root handlers and a leaked handler
would pollute the rest of the session.

One typing fact every setup depends on: ``ObservabilityCfg`` is
``ConfigDict(strict=True, extra="forbid")`` and ``log_to_file`` is typed
``str | None``, NOT ``Path | None``. Under strict mode a ``pathlib.Path``
is rejected with ``ValidationError``, so every path goes in as ``str``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from phantom.config.settings import ObservabilityCfg
from phantom.observability.logging import (
    BearerRedactionFilter,
    SensitiveCaptureRedactor,
    configure_logging,
)

# A token in the shape the bearer filter exists to scrub.
_RAW_TOKEN = "abc.def-123"
_BEARER_LINE = f"Bearer {_RAW_TOKEN}"
_REDACTED_BEARER = "Bearer <redacted>"

# An ordinary record, distinctive enough to grep out of a stream.
_ORDINARY = "phantom-f15-ordinary-record"


@pytest.fixture
def restore_root_logger() -> Iterator[None]:
    """Save and restore the root logger's handlers and level around a test."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        yield
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        for handler in saved_handlers:
            root.addHandler(handler)
        root.setLevel(saved_level)


def _emit(message: str) -> None:
    """Emit one INFO record through a module logger, as production does."""
    logging.getLogger("phantom.test.f15").info(message)


def test_stdout_sink_receives_records_when_enabled(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """The default that today's code contradicts on every deployment.

    Objective: ``log_to_stdout: true`` must actually put records on
    ``sys.stdout``. Pre-fix the bare ``StreamHandler()`` defaulted to
    stderr, so the knob's own description was false. Success is the
    record on ``out`` and NOT on ``err``.
    """
    configure_logging(ObservabilityCfg(log_level="INFO", log_to_stdout=True))
    _emit(_ORDINARY)

    captured = capsys.readouterr()
    assert _ORDINARY in captured.out
    assert _ORDINARY not in captured.err


def test_file_sink_writes_records_and_carries_both_filters(
    restore_root_logger: None, tmp_path: Path
) -> None:
    """The knob that does nothing today, plus the ADR-004 rule for it.

    Objective: ``log_to_file`` must create and append to the named path,
    and it must carry the SAME redaction pair as the stdout sink. The
    file is the sink where this matters most: a console leak scrolls
    away, a file leak persists for the retention of the volume.

    Success: the file exists, holds the ordinary record, holds the
    redacted bearer, and does NOT hold the raw token.
    """
    log_path = tmp_path / "phantom.log"
    configure_logging(
        ObservabilityCfg(log_level="INFO", log_to_stdout=False, log_to_file=str(log_path))
    )
    _emit(_ORDINARY)
    _emit(_BEARER_LINE)
    logging.shutdown()

    text = log_path.read_text(encoding="utf-8")
    assert _ORDINARY in text
    assert _REDACTED_BEARER in text
    assert _RAW_TOKEN not in text


def test_both_sinks_receive_the_same_record(
    restore_root_logger: None, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The file sink is SECONDARY, not exclusive.

    Objective: configuring a file must not silently take the console
    away. Success is one record reaching stdout and the file.
    """
    log_path = tmp_path / "phantom.log"
    configure_logging(
        ObservabilityCfg(log_level="INFO", log_to_stdout=True, log_to_file=str(log_path))
    )
    _emit(_ORDINARY)

    assert _ORDINARY in capsys.readouterr().out
    assert _ORDINARY in log_path.read_text(encoding="utf-8")


def test_no_sinks_installs_a_null_handler_and_emits_nothing(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """No sinks means silence, and the NullHandler is what makes it safe.

    Objective: ``log_to_stdout: false`` with no ``log_to_file`` is a
    legal operator choice, but an EMPTY root handler list is not. Python
    then falls back to ``logging.lastResort``, which emits WARNING and
    above to stderr with no formatter and, decisively, NO redaction
    filters. The one configuration that looks like "no logs" would be the
    only configuration able to print an unredacted bearer. A
    ``NullHandler`` makes the choice mean silence instead.

    Success: nothing on either stream for a WARNING (the level
    ``lastResort`` would have printed), and exactly one root handler,
    a ``logging.NullHandler``.
    """
    configure_logging(ObservabilityCfg(log_level="INFO", log_to_stdout=False))
    logging.getLogger("phantom.test.f15").warning(_ORDINARY)

    captured = capsys.readouterr()
    assert _ORDINARY not in captured.out
    assert _ORDINARY not in captured.err
    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.NullHandler)


def test_unopenable_log_path_is_reported_and_does_not_crash(
    restore_root_logger: None, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A bad log path is recoverable config, not a boot failure.

    Objective: Phantom does not create the parent directory (choosing
    where an operator's logs land, and with what permissions, is not
    Phantom's call), so an unopenable path must be an ERROR through
    whichever sink DID install rather than a crash. The process can serve
    uploads perfectly well while logging to stdout.

    Success: the call returns, the stdout sink is installed, and an ERROR
    naming the path reaches stdout. The OS error text is deliberately not
    asserted, because it differs across platforms.
    """
    missing = tmp_path / "missing-dir" / "phantom.log"
    configure_logging(
        ObservabilityCfg(log_level="INFO", log_to_stdout=True, log_to_file=str(missing))
    )
    _emit(_ORDINARY)

    captured = capsys.readouterr()
    assert str(missing) in captured.out
    assert _ORDINARY in captured.out


@pytest.mark.parametrize(
    "observability",
    [
        ObservabilityCfg(log_level="DEBUG", log_to_stdout=True),
        ObservabilityCfg(log_level="DEBUG", log_to_stdout=False),
    ],
    ids=["stdout", "no-sinks"],
)
def test_sink_selection_does_not_disturb_the_dependency_floor(
    restore_root_logger: None, observability: ObservabilityCfg
) -> None:
    """The leak boundary must survive the rewrite, for every sink set.

    Objective: the boundary for records whose secrets are interpolated
    through non-string args (which string-level redaction cannot reach) is
    the ROOT level, which is every third-party logger's effective level
    because none of them set one. It is applied AFTER the sink selection
    precisely so an operator DEBUG never re-opens that surface, and that
    must hold for every sink combination, not just the default one.

    Success: at ``log_level: DEBUG`` the effective level of a dependency
    logger is the WARNING floor while Phantom's own tree is at DEBUG.
    """
    configure_logging(observability)

    for dependency in ("aiosqlite", "httpx", "httpcore", "botocore.auth"):
        assert logging.getLogger(dependency).getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("phantom.workers.sender").getEffectiveLevel() == logging.DEBUG


def test_log_level_still_reaches_phantoms_own_loggers(restore_root_logger: None) -> None:
    """The one knob that already worked must keep working.

    Objective: F15 changed the signature and the sink set and the dependency
    floor changed which logger carries ``log_level``; none of it may disturb
    the knob itself, which must still set the level Phantom's own records are
    emitted at.
    """
    configure_logging(ObservabilityCfg(log_level="WARNING", log_to_stdout=True))

    assert logging.getLogger("phantom.config.settings").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger().level == logging.WARNING


def test_every_installed_handler_carries_both_filters(
    restore_root_logger: None, tmp_path: Path
) -> None:
    """Redaction is per handler, so a second sink must not be a hole.

    Objective: the filters have always been attached per handler rather
    than to the logger, and multiplying the handlers is exactly how that
    shape could silently regress. Success: with both sinks configured,
    EVERY root handler carries both filter classes.
    """
    configure_logging(
        ObservabilityCfg(
            log_level="INFO",
            log_to_stdout=True,
            log_to_file=str(tmp_path / "phantom.log"),
        )
    )

    handlers = logging.getLogger().handlers
    assert len(handlers) == 2
    for handler in handlers:
        kinds = {type(f) for f in handler.filters}
        assert BearerRedactionFilter in kinds
        assert SensitiveCaptureRedactor in kinds


def test_a_botocore_signature_never_reaches_the_persistent_file_sink(
    restore_root_logger: None, tmp_path: Path
) -> None:
    """The leak the allowlist shape could not have prevented.

    Objective: ``botocore.auth``'s logger is NOTSET, so it inherited the root
    level, and at ``log_level: DEBUG`` every SigV4 signing attempt wrote the
    live STS session token, the derived signature and the full canonical
    request into EVERY sink - including ``log_to_file``, which persists for
    the retention of the volume. ``BearerRedactionFilter`` matches
    ``Bearer\\s+...`` and touches none of it, so the level boundary is the
    only thing that can stop it.

    This is driven through the REAL ``configure_logging`` and the REAL
    botocore signer, because the defect was precisely that the constant
    enumerating capped loggers did not list botocore: a test against the
    constant would have agreed with the bug.

    Success: the file sink holds none of the credential material.
    """
    import botocore.auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    session_token = "STS-SESSION-TOKEN-Kp4-SENTINEL"
    secret_key = "SECRET-ACCESS-KEY-Vq7-SENTINEL"
    log_path = tmp_path / "phantom.log"
    configure_logging(
        ObservabilityCfg(log_level="DEBUG", log_to_stdout=False, log_to_file=str(log_path))
    )

    signer = botocore.auth.SigV4Auth(
        Credentials("AKIAEXAMPLE", secret_key, token=session_token),
        "s3",
        "us-east-1",
    )
    request = AWSRequest(
        method="PUT",
        url="https://bucket.s3.amazonaws.com/key",
        data=b"body",
        headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
    )
    signer.add_auth(request)
    logging.shutdown()

    text = log_path.read_text(encoding="utf-8")
    assert session_token not in text, "STS session token leaked (value suppressed)"
    assert secret_key not in text, "secret access key leaked (value suppressed)"
    assert "CanonicalRequest" not in text, "botocore's signing DEBUG record reached the sink"


def test_a_dependency_added_tomorrow_is_inside_the_boundary(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """The boundary is a default, not a list, so nothing has to be remembered.

    Objective: the previous shape was an ALLOWLIST of three named dependency
    loggers, which is only as complete as the last audit - botocore was the
    fourth that got missed. Inverting it to default-deny means a logger nobody
    has heard of is capped by construction.

    Success: an arbitrary never-enumerated third-party logger emits nothing at
    DEBUG while Phantom's own tree does.
    """
    configure_logging(ObservabilityCfg(log_level="DEBUG", log_to_stdout=True))

    logging.getLogger("some.future.dependency").debug("dependency-debug-sentinel")
    logging.getLogger("phantom.test.f15").debug("phantom-debug-sentinel")

    out = capsys.readouterr().out
    assert "dependency-debug-sentinel" not in out
    assert "phantom-debug-sentinel" in out


def test_a_dependency_warning_still_reaches_the_operator(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """Default-deny must cap the chatter, not silence the dependency.

    Objective: a floor set too high would hide the dependency records an
    operator actually needs - a connection failure, a retry exhaustion. The
    floor is WARNING for exactly that reason. Success: a dependency WARNING
    is emitted even though its DEBUG is not.
    """
    configure_logging(ObservabilityCfg(log_level="DEBUG", log_to_stdout=True))

    logging.getLogger("some.future.dependency").warning("dependency-warning-sentinel")

    assert "dependency-warning-sentinel" in capsys.readouterr().out


def test_an_operator_error_level_is_not_loosened_by_the_floor(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """The floor caps dependencies; it must never raise their volume.

    Objective: the floor is applied with ``max``, not by assignment, so an
    operator who asks for ERROR gets ERROR everywhere rather than having
    WARNING-level dependency records reappear. Success: a dependency WARNING
    is silent at ``log_level: ERROR``.
    """
    configure_logging(ObservabilityCfg(log_level="ERROR", log_to_stdout=True))

    logging.getLogger("some.future.dependency").warning("dependency-warning-sentinel")
    logging.getLogger("some.future.dependency").error("dependency-error-sentinel")

    out = capsys.readouterr().out
    assert "dependency-warning-sentinel" not in out
    assert "dependency-error-sentinel" in out


def test_the_launcher_logger_is_inside_phantoms_own_tree(
    restore_root_logger: None, capsys: pytest.CaptureFixture[str]
) -> None:
    """``python -m phantom`` logs under ``__main__``, not under ``phantom``.

    Objective: the entry point is executed as ``__main__``, so
    ``logging.getLogger(__name__)`` there produces a logger OUTSIDE the
    package tree. A default-deny boundary that only lifted ``phantom`` would
    have capped the launcher's own records at WARNING. Success: its DEBUG is
    emitted at ``log_level: DEBUG``.
    """
    configure_logging(ObservabilityCfg(log_level="DEBUG", log_to_stdout=True))

    logging.getLogger("__main__").debug("launcher-debug-sentinel")

    assert "launcher-debug-sentinel" in capsys.readouterr().out
