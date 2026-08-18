"""F15's M2 witness: the sink config reaches the process through create_app.

``configure_logging`` is called in ``create_app``'s body, before the
lifespan runs, so this needs no server and no lifespan. It is the witness
rather than any test in ``test_logging_sinks.py`` because it uses only
today's public API: on the pre-fix tree it fails BEHAVIOURALLY (no file
is created, because no ``FileHandler`` exists anywhere in the service)
rather than raising a ``TypeError`` from a signature that does not exist
yet.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from phantom.app import create_app
from phantom.config.settings import Settings

_INSTANCE_ID = "inst-a"
_BOOT_HOST = "files.example.com"

# Emitted after create_app returns, to prove the sink is live rather than
# merely created.
_AFTER_BOOT_RECORD = "phantom-f15-after-create-app"


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


def _settings_with_file_sink(tmp_path: Path, log_path: Path) -> Settings:
    """Load a minimal valid Settings whose observability names a file sink.

    The path goes in as a ``str``: ``ObservabilityCfg`` is
    ``strict=True`` and ``log_to_file`` is typed ``str | None``, so a
    ``pathlib.Path`` is rejected with a ``ValidationError``.
    """
    raw: dict[str, Any] = {
        "storage": {"data_dir": str(tmp_path / "data")},
        "observability": {"log_level": "INFO", "log_to_file": str(log_path)},
        "instances": [
            {
                "id": _INSTANCE_ID,
                "host_prefixes": [_BOOT_HOST],
                "data_dir": _INSTANCE_ID,
                "routes": [
                    {
                        "name": "files",
                        "hosts": [_BOOT_HOST],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
    }
    settings_path = tmp_path / "phantom.yaml"
    settings_path.write_text(yaml.safe_dump(raw))
    return Settings.reload_from_yaml(settings_path)


def test_create_app_installs_the_configured_file_sink(
    restore_root_logger: None, tmp_path: Path
) -> None:
    """The config knob reaches the process through the real composition root.

    Objective: prove F15 end to end at the wiring level, with today's
    public API only. ``create_app`` calls ``configure_logging`` in its
    body, so the sink must exist the moment it returns.

    Success: the configured file exists after ``create_app`` returns, and
    a record emitted afterwards lands in it.

    Pre-fix failure mode: no file is ever created, so the existence
    assertion fails. Nothing raises, and nothing that does not exist is
    imported.
    """
    log_path = tmp_path / "phantom.log"
    settings = _settings_with_file_sink(tmp_path, log_path)

    create_app(settings)

    assert log_path.exists(), (
        "create_app must install the configured log_to_file sink; the file was never created"
    )
    logging.getLogger("phantom.test.f15").info(_AFTER_BOOT_RECORD)
    logging.shutdown()
    assert _AFTER_BOOT_RECORD in log_path.read_text(encoding="utf-8")
