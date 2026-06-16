"""Logging-discipline tests.

The emulator is test infrastructure and the HS256 default secret is
known. Even so, the production rule is "never log raw secrets or full
JWT bodies." These tests assert that the modules use level-aware
loggers (no print()) and that bearer / signing-key strings do not
appear in INFO-level output when an upload flows end-to-end.
"""

from __future__ import annotations

import io
import logging
import re

import httpx
import pytest
from phantom_emulator import AppConfig, start_server
from phantom_emulator.config import ServerCfg


@pytest.fixture(autouse=True)
def _emulator_signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)


async def test_secret_redaction(caplog: pytest.LogCaptureFixture) -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    caplog.set_level(logging.INFO, logger="phantom_emulator")
    try:
        async with httpx.AsyncClient(base_url=server.url()) as client:
            token_r = await client.post(
                "/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": "test-client",
                    "client_secret": "test-secret",
                },
            )
            jwt = token_r.json()["access_token"]
            await client.post(
                "/v1/files/create",
                json={
                    "domain": "D",
                    "fileName": "f",
                    "metadata": {"keyValueStore": {}},
                },
                headers={"Authorization": f"Bearer {jwt}"},
            )
    finally:
        await server.stop()

    log_output = "\n".join(record.getMessage() for record in caplog.records)
    # Full JWT body must not be in INFO logs.
    assert jwt not in log_output, "raw JWT leaked into INFO logs"
    # Raw client secret must not be in INFO logs.
    assert "test-secret" not in log_output
    # The HS256 signing-key shared secret must not be in INFO logs.
    assert ("x" * 32) not in log_output


def test_no_print_in_source_tree() -> None:
    """No module uses ``print()`` in production code.

    Walks the package source and asserts that ``print(`` does not appear
    in any production module (excluding tests).
    """
    import phantom_emulator

    root = phantom_emulator.__path__[0]
    bad: list[str] = []
    import os

    for dirpath, _dirs, files in os.walk(root):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding="utf-8") as fh:
                content = fh.read()
            # `re.search` so we don't trip on words that contain "print".
            if re.search(r"(^|[^\w])print\s*\(", content):
                bad.append(path)
    assert not bad, f"print() found in: {bad}"


def test_modules_use_module_named_logger() -> None:
    """Every production module declares ``logger = logging.getLogger(__name__)``."""
    import phantom_emulator

    root = phantom_emulator.__path__[0]
    missing: list[str] = []
    import os

    for dirpath, _dirs, files in os.walk(root):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            if filename in {"__init__.py", "__main__.py"}:
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding="utf-8") as fh:
                content = fh.read()
            if "logging.getLogger(__name__)" not in content:
                # Some leaf modules (e.g., a pure enum) may not log;
                # require getLogger only where the module's body is
                # non-trivial. Treat <50 lines as trivial.
                line_count = content.count("\n")
                if line_count > 50:
                    missing.append(path)
    assert not missing, f"missing logger declaration: {missing}"


def _flush_stream(stream: io.StringIO) -> str:
    """Return and clear ``stream``."""
    value = stream.getvalue()
    stream.truncate(0)
    stream.seek(0)
    return value
