"""Range and sentinel guards the settings model owes its consumers.

Four defects, all the same shape: a field whose accepted range was wider than
the meaning its documentation claimed, so a config that VALIDATED - and passed
``python -m phantom --validate``, and exported into
``contracts/settings.schema.json`` - produced behaviour the operator did not
ask for, with no boot-time signal.

* ``body_store.ram_ceiling_bytes: 0``. The reference config documented 0 as
  "no RAM bodies"; the only consumer (``RamPressureWatcher._check_once``)
  early-returns on ``<= 0`` and DISABLES ceiling enforcement, so RAM grew
  unbounded to OOM. CONTEXT.md calls the ceiling "an enforced bound, not a
  best-effort gauge", and ``saturation.max_in_flight_bytes`` one class away
  already states in its own description that 0 is not an unlimited sentinel.
* ``compression.level``. ``ge=1`` with no upper bound, so ``{zstd, 30}``
  validated, then raised ``ValueError`` inside the codec on the FIRST upload
  and on every upload after it: a service that is up, healthy-looking, and
  accepting nothing. ADR-013 lists the knob as live-read, so it can also
  arrive by admin reload with no boot to catch it.
* ``retry.default_strategy.max_attempts``. No floor at all, so a typo ``-5``
  validated and silently meant UNBOUNDED retries, because the strategy tests
  ``0 <= max_attempts <= attempts``. That is the exact failure mode
  ``RetentionCfg`` says it hardened every window against with ``ge=-1``.
* ``retry.default_strategy.max_duration_seconds``. The mirror image: the
  strategy's guard and docstring both promise ``-1 = unbounded``, but ``ge=0``
  made -1 a validation error, so unbounded duration was unreachable from
  config.

Plus the one credential this config holds as a literal:
``server.tls.key_password`` was typed plain ``str``, so ``--validate`` - which
``__main__.py`` advertises as safe to run at deploy time - wrote the
passphrase to stdout via ``model_dump_json`` and into
``contracts/settings.schema.json`` as an unmarked string.
"""

from __future__ import annotations

import json

import pytest
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    RetryStrategyCfg,
    Settings,
    TlsCfg,
)
from phantom.strategies import build_retry_strategy
from pydantic import SecretStr, ValidationError

# The withdrawn "no RAM bodies" sentinel.
_WITHDRAWN_CEILING_SENTINEL = 0

# A compression level no codec accepts: zstd tops out at 22, gzip at 9.
_OUT_OF_RANGE_LEVEL = 30

# A typo below the -1 sentinel. Must be a loud error for every budget knob,
# never a silent "forever".
_BELOW_SENTINEL = -5


def test_the_ram_ceiling_rejects_the_withdrawn_zero_sentinel() -> None:
    """0 must not be a way to switch ceiling enforcement off.

    Objective: the ceiling is an enforced bound (CONTEXT.md), and the only
    consumer read 0 as "no ceiling" - the opposite of the "no RAM bodies" the
    reference config documented. A value that disables the bound cannot be
    accepted, so 0 is rejected and the error points at the real mechanism for
    that posture.

    Success: ``ValidationError``, and the message names ``all_disk`` so an
    operator following the old documentation is told where to go.
    """
    with pytest.raises(ValidationError) as caught:
        BodyStoreCfg(ram_ceiling_bytes=_WITHDRAWN_CEILING_SENTINEL)

    assert "all_disk" in str(caught.value)


def test_a_real_ram_ceiling_and_an_unset_one_are_both_accepted() -> None:
    """Rejecting 0 must not disturb the two legitimate shapes.

    Objective: ``None`` means "probe-fill at startup" (the documented
    smart-defaults posture) and any positive byte count is an operator pin.
    Success: both validate.
    """
    assert BodyStoreCfg(ram_ceiling_bytes=None).ram_ceiling_bytes is None
    assert BodyStoreCfg(ram_ceiling_bytes=536_870_912).ram_ceiling_bytes == 536_870_912


@pytest.mark.parametrize(
    ("algorithm", "highest_accepted"),
    [("zstd", 22), ("gzip", 9)],
)
def test_the_compression_level_is_bounded_by_its_algorithm(
    algorithm: str, highest_accepted: int
) -> None:
    """The codec's own range must be enforced where the config is read.

    Objective: the codec is a per-admission thunk, never built at boot, so an
    out-of-range level is not discovered until the first upload - by which
    time the service is up and healthy-looking and every upload fails. The
    model already carries everything needed to decide: ``algorithm`` is a
    sibling ``Literal`` on the same class.

    Success: the highest value the library accepts validates, one past it does
    not, and the error names both the level and the algorithm.
    """
    assert CompressionCfg(algorithm=algorithm, level=highest_accepted).level == highest_accepted  # type: ignore[arg-type]  # parametrised with literal members

    with pytest.raises(ValidationError) as caught:
        CompressionCfg(algorithm=algorithm, level=highest_accepted + 1)  # type: ignore[arg-type]  # parametrised with literal members

    message = str(caught.value)
    assert str(highest_accepted + 1) in message
    assert algorithm in message


def test_an_out_of_range_level_is_rejected_for_every_real_codec() -> None:
    """The specific value from the finding, against both real codecs."""
    for algorithm in ("zstd", "gzip"):
        with pytest.raises(ValidationError):
            CompressionCfg(algorithm=algorithm, level=_OUT_OF_RANGE_LEVEL)  # type: ignore[arg-type]  # literal members


def test_the_passthrough_codec_reads_no_level_and_bounds_none() -> None:
    """``original`` is the identity codec: it has no level to be out of range.

    Objective: the bound is the LIBRARY's, not a Phantom policy, so it must
    not be invented for a codec that reads no level. Success: a high level
    validates under ``original``.
    """
    assert CompressionCfg(algorithm="original", level=_OUT_OF_RANGE_LEVEL).level == (
        _OUT_OF_RANGE_LEVEL
    )


@pytest.mark.parametrize("field", ["max_attempts", "max_duration_seconds"])
def test_a_retry_budget_typo_below_the_sentinel_is_a_loud_error(field: str) -> None:
    """The RetentionCfg hardening, applied to the block that was missed.

    Objective: ``-1`` is the ONE unbounded sentinel for both retry budgets.
    ``max_attempts`` carried no floor at all, so ``-5`` validated and, through
    the strategy's ``0 <= max_attempts <= attempts`` guard, silently meant
    unbounded retries. ``RetentionCfg``'s docstring says every retention
    window got ``ge=-1`` so exactly this typo would be loud; the retry block
    never got the same guard.

    Success: ``-5`` is a ``ValidationError`` for both budget knobs.
    """
    with pytest.raises(ValidationError):
        RetryStrategyCfg(**{field: _BELOW_SENTINEL})


def test_unbounded_duration_is_reachable_from_config() -> None:
    """The sentinel the strategy promised but the model refused.

    Objective: ``ExponentialBackoffStrategy``'s docstring and its
    ``0 <= max_duration <= elapsed`` guard both promise "-1 = unbounded", but
    ``ge=0`` made -1 a validation error, so unbounded duration could not be
    configured at all. Success: -1 validates and the built strategy keeps
    scheduling past what any finite budget would have stopped.
    """
    cfg = RetryStrategyCfg(max_duration_seconds=-1, max_attempts=-1)
    assert cfg.max_duration_seconds == -1

    from datetime import timedelta

    strategy = build_retry_strategy(cfg)
    delay = strategy.schedule_next_attempt(
        attempts=3,
        since_received=timedelta(days=30),
        last_error=None,
        route_name="r",
    )
    assert delay is not None, "-1 must mean unbounded, not 'expired 30 days ago'"


def test_zero_budgets_are_accepted_and_mean_no_retry() -> None:
    """The documented meaning of the other edge value.

    Objective: ``0`` is accepted for both budgets and means "no retry budget
    at all" - the strategy's guards give up on the first scheduling call.
    Documenting it is the point: it was previously accepted for
    ``max_duration_seconds`` with nothing saying what it did.

    Success: both validate at 0 and the built strategy returns ``None``
    immediately.
    """
    from datetime import timedelta

    for cfg in (
        RetryStrategyCfg(max_attempts=0),
        RetryStrategyCfg(max_duration_seconds=0),
    ):
        strategy = build_retry_strategy(cfg)
        assert (
            strategy.schedule_next_attempt(
                attempts=0,
                since_received=timedelta(0),
                last_error=None,
                route_name="r",
            )
            is None
        )


def test_the_tls_passphrase_is_a_secret_str() -> None:
    """The one literal credential in the config must not be a plain string.

    Objective: ``server.tls.key_password`` is the sole config field holding a
    credential LITERAL rather than the name of an environment variable, and
    ``python -m phantom --validate`` - documented as safe to run at deploy
    time - dumps the resolved settings to stdout. As a plain ``str`` that put
    the passphrase into CI job logs and deploy transcripts verbatim.

    Success: the field holds a ``SecretStr``, the literal is recoverable by
    the consumer, and the ``--validate`` dump does not contain it.
    """
    passphrase = "tls-passphrase-Q7-literal"
    tls = TlsCfg(enabled=False, key_password=passphrase)  # type: ignore[arg-type]  # SecretStr accepts a str

    assert isinstance(tls.key_password, SecretStr)
    assert tls.key_password is not None
    assert tls.key_password.get_secret_value() == passphrase
    assert passphrase not in tls.model_dump_json()

    dump = Settings(server={"tls": {"key_password": passphrase}}).model_dump_json()  # type: ignore[arg-type]  # sub-block as a mapping
    assert passphrase not in dump, "--validate would have printed the passphrase"


def test_the_exported_contract_marks_the_passphrase_as_a_secret() -> None:
    """The contract is the other publication channel, and it leaked too.

    Objective: ``contracts/settings.schema.json`` is generated from this model
    and is committed, so a plain-``str`` passphrase field also told every
    implementation built from the contract that this is an ordinary string.

    Success: the exported property carries the ``format: password`` /
    ``writeOnly`` markers pydantic emits for ``SecretStr``. The assertion
    reads the string arm of the ``anyOf`` rather than the whole property,
    because the property's own ``description`` mentions the word "password"
    and would satisfy a looser check while the field was still a plain string.
    """
    schema = Settings.model_json_schema()
    tls_schema = schema["$defs"]["TlsCfg"]["properties"]["key_password"]
    string_arm = next(arm for arm in tls_schema["anyOf"] if arm.get("type") == "string")

    assert string_arm.get("format") == "password", json.dumps(string_arm)
    assert string_arm.get("writeOnly") is True, json.dumps(string_arm)
