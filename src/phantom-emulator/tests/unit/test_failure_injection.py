"""Unit tests for :mod:`phantom_emulator.failure.injection`."""

from __future__ import annotations

from phantom_emulator.failure.injection import (
    FailureInjectionState,
    FailurePolicy,
    FailureScope,
)


def test_set_clear() -> None:
    state = FailureInjectionState(seed=0)
    pol = FailurePolicy(scope=FailureScope.UPSTREAM_FILES_CREATE, error_rate_5xx=0.5)
    state.set_policy(pol)
    assert state.resolve(FailureScope.UPSTREAM_FILES_CREATE) is pol
    event = state.record_error_rate_5xx(FailureScope.UPSTREAM_FILES_CREATE)
    assert event.scope is FailureScope.UPSTREAM_FILES_CREATE
    assert state.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_CREATE) == 1
    state.clear_all()
    assert state.resolve(FailureScope.UPSTREAM_FILES_CREATE) is None
    assert state.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_CREATE) == 0


def test_most_specific_wins() -> None:
    state = FailureInjectionState(seed=0)
    global_pol = FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=0.1)
    specific_pol = FailurePolicy(scope=FailureScope.UPSTREAM_FILES_UPLOAD, error_rate_5xx=0.9)
    state.set_policy(global_pol)
    state.set_policy(specific_pol)

    # Specific scope returns its own policy.
    assert state.resolve(FailureScope.UPSTREAM_FILES_UPLOAD) is specific_pol
    # Unconfigured scope falls back to global.
    assert state.resolve(FailureScope.AUTH_TOKEN) is global_pol


def test_no_policy_returns_none() -> None:
    state = FailureInjectionState(seed=0)
    assert state.resolve(FailureScope.AUTH_TOKEN) is None


def test_call_count_increments() -> None:
    state = FailureInjectionState(seed=0)
    assert state.record_call(FailureScope.UPSTREAM_FILES_CREATE) == 1
    assert state.record_call(FailureScope.UPSTREAM_FILES_CREATE) == 2
    assert state.record_call(FailureScope.UPSTREAM_FILES_UPLOAD) == 1


def test_error_rate_5xx_observations_are_typed_per_scope_and_separate() -> None:
    """5xx observations neither alias scopes nor mutate auth call counts."""
    state = FailureInjectionState(seed=0)
    first = state.record_error_rate_5xx(FailureScope.UPSTREAM_FILES_CREATE)
    second = state.record_error_rate_5xx(FailureScope.UPSTREAM_FILES_CREATE)
    upload = state.record_error_rate_5xx(FailureScope.UPSTREAM_FILES_UPLOAD)

    assert state.error_rate_5xx_for_scope(FailureScope.UPSTREAM_FILES_CREATE) == (first, second)
    assert state.error_rate_5xx_for_scope(FailureScope.UPSTREAM_FILES_UPLOAD) == (upload,)
    assert state.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_CREATE) == 2
    assert state.call_counts == {}


def test_seeded_rng_deterministic() -> None:
    a = FailureInjectionState(seed=42)
    b = FailureInjectionState(seed=42)
    a_vals = [a.rng.random() for _ in range(5)]
    b_vals = [b.rng.random() for _ in range(5)]
    assert a_vals == b_vals


def test_set_seed_resets_sequence() -> None:
    state = FailureInjectionState(seed=0)
    first = state.rng.random()
    state.set_seed(0)
    second = state.rng.random()
    assert first == second
    assert state.seed == 0

    state.set_seed(7)
    assert state.seed == 7


def test_policy_serialization_roundtrip() -> None:
    pol = FailurePolicy(
        scope=FailureScope.UPSTREAM_FILES_UPLOAD,
        error_rate_5xx=0.25,
        auth_401_after_n_calls=3,
        latency_ms=100,
    )
    raw = pol.model_dump_json()
    parsed = FailurePolicy.model_validate_json(raw)
    assert parsed == pol
