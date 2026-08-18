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
def test_sink_selection_does_not_disturb_the_dependency_caps(
    restore_root_logger: None, observability: ObservabilityCfg
) -> None:
    """The pre-existing leak boundary must survive the rewrite.

    Objective: ``_DEPENDENCY_LOG_CAPS`` is the boundary for records whose
    secrets are interpolated through non-string args, so string-level
    redaction cannot reach them. The caps are applied AFTER the root
    level precisely so an operator DEBUG never re-opens that surface, and
    that must hold for every sink combination, not just the default one.
    """
    configure_logging(observability)

    assert logging.getLogger("aiosqlite").level == logging.INFO
    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_log_level_still_reaches_the_root_logger(restore_root_logger: None) -> None:
    """The one knob that already worked must keep working.

    Objective: F15 changes the signature and the sink set; it must not
    disturb ``log_level``, which is the only one of the three knobs that
    was ever consulted.
    """
    configure_logging(ObservabilityCfg(log_level="WARNING", log_to_stdout=True))

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
