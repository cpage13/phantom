"""Unit tests for :mod:`phantom_emulator.state`."""

from __future__ import annotations

from datetime import UTC, datetime

from phantom_emulator.config import AppConfig
from phantom_emulator.state import EmulatorState


def test_initial_state() -> None:
    cfg = AppConfig()
    started = datetime.now(UTC)
    state = EmulatorState(cfg=cfg, started_at=started)

    assert state.cfg is cfg
    assert state.started_at == started
    assert state.issued_tokens == {}
    assert state.pending_uploads == {}
    assert state.accepted_bodies == {}
    assert state.idempotency_cache == {}
    assert state.global_paused is False
    assert state.seed == 0
    assert state.failure_state is None
    assert state.jwt_minter is None
    assert state.rsa_keys is None
    assert state.auth_mode_overrides == {}
