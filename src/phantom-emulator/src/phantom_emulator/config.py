"""Application configuration for the phantom-emulator.

Pydantic models define the full configuration surface. Defaults are
chosen so that an empty YAML (or no YAML at all) produces a working
emulator. Environment variables prefixed ``PHANTOM_EMULATOR_`` may
overlay any value using ``__`` as the nested delimiter - for example
``PHANTOM_EMULATOR_AUTH__SIGNING__MODE=RS256``.

See plan §4.1, §7.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]  # types-PyYAML not in workspace dev deps
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from phantom_emulator.auth.modes import AuthMode

logger = logging.getLogger(__name__)

# Default listen port for Docker mode. 0 = OS-assigned, used in wheel mode.
DEFAULT_PORT: int = 8000

# Default JWT expiration. Matches Microsoft AD's typical access-token life.
DEFAULT_EXPIRES_IN_SECONDS: int = 3600

# Default clock-skew tolerance. Matches real AD's published ±5 min default.
DEFAULT_CLOCK_SKEW_SECONDS: int = 300

# Default presigned URL TTL. Real upstream presigned URLs typically live 1h.
DEFAULT_PRESIGNED_TTL_SECONDS: int = 3600

# Per-PUT body size cap. Matches Phantom's max_buffered_bytes default
# (2 GiB) so the emulator handles every supported upload size without
# overlay during E2E.
DEFAULT_BODY_MAX_BYTES: int = 2_147_483_648

# Idempotency-Key dedup cache window. Aligned with the default presigned
# URL TTL so a re-execution within the URL's life always hits the cache.
DEFAULT_IDEMPOTENCY_DEDUP_WINDOW_SECONDS: int = 3_600

# Conventional "all-zeros" tenant id used as the default JWT `tid` claim
# so tests don't accidentally rely on a real tenant value.
DEFAULT_TENANT_ID: str = "00000000-0000-0000-0000-000000000000"


class ServerCfg(BaseModel):
    """HTTP listen configuration."""

    model_config = ConfigDict(extra="forbid")

    host: str = Field("0.0.0.0", description="Listen host.")
    port: int = Field(
        DEFAULT_PORT,
        ge=0,
        le=65535,
        description="Listen port. 0 = OS-assigned (used in wheel mode).",
    )


class AuthSigningCfg(BaseModel):
    """JWT signing parameters.

    HS256 mode uses a shared secret read from the env var named by
    ``hs256_secret_env``. RS256 mode generates an RSA keypair, persists
    it for the session by default, and publishes the public half at
    ``/.well-known/jwks.json``.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["HS256", "RS256"] = Field("HS256", description="JWT signing algorithm.")
    hs256_secret_env: str = Field(
        "EMULATOR_SIGNING_KEY",
        description="HS256 mode: env var holding the shared secret.",
    )
    rs256_keypair: Literal["auto", "rotate-on-startup"] | str = Field(
        "auto",
        description=(
            "RS256 mode: 'auto' (generate at startup, persist for "
            "session), 'rotate-on-startup' (force re-generation each "
            "startup), or a PEM path."
        ),
    )


class AuthClient(BaseModel):
    """Configured OAuth2 client-credentials pair.

    The emulator validates inbound ``client_id``/``client_secret``
    against the list configured under :attr:`AuthCfg.clients`.
    """

    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(..., description="Public client identifier.")
    client_secret: str = Field(..., description="Shared secret.")


class AuthCfg(BaseModel):
    """OAuth2 / JWT minting configuration."""

    model_config = ConfigDict(extra="forbid")

    default_mode: AuthMode = Field(
        AuthMode.OAUTH_CLIENT_CREDENTIALS,
        description="Default authentication mode for upstream endpoints.",
    )
    # `default_factory=<BaseModel subclass>` is the idiomatic Pydantic way to
    # build a nested-default; without the Pydantic mypy plugin (which is
    # incompatible with this workspace's mypy 2.0 + Pydantic 2.13 combination),
    # mypy can't bind the callable's return type against the field's declared
    # type and emits `arg-type`. The runtime behavior is correct.
    signing: AuthSigningCfg = Field(
        default_factory=AuthSigningCfg,  # type: ignore[arg-type]
        description="JWT signing algorithm and key material.",
    )
    issuer: str = Field("http://emulator", description="JWT iss claim.")
    audience: str = Field(
        "api://emulator/.default",
        description="JWT aud claim.",
    )
    default_expires_in_seconds: int = Field(
        DEFAULT_EXPIRES_IN_SECONDS,
        ge=1,
        description="JWT lifetime (exp - iat) in seconds.",
    )
    clock_skew_seconds: int = Field(
        DEFAULT_CLOCK_SKEW_SECONDS,
        ge=0,
        description="Default ±5 min, matching real AD.",
    )
    tenant_id: str = Field(DEFAULT_TENANT_ID, description="JWT tid claim.")
    clients: list[AuthClient] = Field(
        default_factory=lambda: [
            AuthClient(client_id="test-client", client_secret="test-secret"),
        ],
        description="Allowed OAuth2 client-credentials pairs.",
    )


class UpstreamCfg(BaseModel):
    """Two-step-upload upstream endpoint configuration."""

    model_config = ConfigDict(extra="forbid")

    presigned_ttl_seconds: int = Field(
        DEFAULT_PRESIGNED_TTL_SECONDS,
        ge=1,
        description="Default lifetime of a minted presigned upload URL.",
    )
    body_max_bytes: int = Field(
        DEFAULT_BODY_MAX_BYTES,
        ge=1,
        description=(
            "Per-PUT body size limit. Default matches Phantom's "
            "max_buffered_bytes so the emulator handles every supported "
            "upload size without overlay."
        ),
    )
    idempotency_dedup_window_seconds: int = Field(
        DEFAULT_IDEMPOTENCY_DEDUP_WINDOW_SECONDS,
        ge=1,
        description=(
            "Idempotency-Key dedup cache window. Same key within the "
            "window returns identical responses. Backs ADR-011 "
            "capture_reexecution=true tests."
        ),
    )


class S3Cfg(BaseModel):
    """Known SigV4 test credentials the path-style PUT/GET validates against.

    The validator (:func:`phantom_emulator.routers.s3._verify_sigv4`)
    recomputes the inbound SigV4 signature and compares. It consumes
    ``access_key_id`` (the credential-id equality check),
    ``secret_access_key`` (the recompute key), and ``body_max_bytes``
    (the 413 cap). ``region`` / ``service`` document the expected
    credential scope but are NOT consumed by the recompute - the
    request's own credential scope drives the comparison.

    Defaults are the public AWS-doc example pair so a test that signs
    client-side with the same pair round-trips with zero overlay.
    Env-overridable via ``PHANTOM_EMULATOR_S3__SECRET_ACCESS_KEY=…``
    (the ``__`` nested delimiter).
    """

    model_config = ConfigDict(extra="forbid")

    access_key_id: str = Field("AKIAIOSFODNN7EXAMPLE", description="Known test access key id.")
    secret_access_key: str = Field(
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        description="Known test secret access key (the AWS-doc example pair).",
    )
    region: str = Field("us-east-1", description="Expected credential-scope region.")
    service: str = Field("s3", description="Expected credential-scope service.")
    body_max_bytes: int = Field(DEFAULT_BODY_MAX_BYTES, ge=1, description="Per-PUT body cap.")


class FailureInjectionCfg(BaseModel):
    """Static failure-injection defaults loaded from YAML.

    Tests typically drive failure injection via the ``/control/*`` API
    rather than YAML; this block stays for symmetry with the rest of
    the config tree and as the natural home for any future
    YAML-locked defaults.
    """

    model_config = ConfigDict(extra="forbid")


class ControlCfg(BaseModel):
    """``/control/*`` surface binding."""

    model_config = ConfigDict(extra="forbid")

    bind: Literal["loopback", "all"] = Field(
        "loopback",
        description=(
            "Where /control/* binds. 'loopback' restricts to 127.0.0.1; "
            "'all' allows cross-host access for Docker-mode E2E."
        ),
    )


class LoggingCfg(BaseModel):
    """Process-wide logging configuration."""

    model_config = ConfigDict(extra="forbid")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        "INFO", description="Root logger level."
    )


class AppConfig(BaseSettings):
    """Top-level emulator configuration.

    Composed from a YAML file, environment overlay, and built-in
    defaults. Environment variables use the prefix
    ``PHANTOM_EMULATOR_`` and the nested delimiter ``__`` - for
    example ``PHANTOM_EMULATOR_AUTH__SIGNING__MODE=RS256``.
    """

    # strict=False lets env-var string values coerce to the underlying
    # numeric / bool types. extra="forbid" still catches typos at the
    # YAML level.
    model_config = SettingsConfigDict(
        env_prefix="PHANTOM_EMULATOR_",
        env_nested_delimiter="__",
        extra="forbid",
        strict=False,
    )

    server: ServerCfg = Field(
        default_factory=ServerCfg,  # type: ignore[arg-type]
        description="HTTP server bind (host + port).",
    )
    auth: AuthCfg = Field(
        default_factory=AuthCfg,  # type: ignore[arg-type]
        description="Auth mode, JWT signing, issuer / audience, and client registrations.",
    )
    upstream: UpstreamCfg = Field(
        default_factory=UpstreamCfg,  # type: ignore[arg-type]
        description="Upstream-facing knobs (presigned TTL, body cap, idempotency window).",
    )
    s3: S3Cfg = Field(
        default_factory=S3Cfg,  # type: ignore[arg-type]
        description="Known SigV4 test creds + region/service for the path-style S3 sink.",
    )
    failure_injection: FailureInjectionCfg = Field(
        default_factory=FailureInjectionCfg,
        description="Static failure-injection config (further runtime control via /control/*).",
    )
    control: ControlCfg = Field(
        default_factory=ControlCfg,  # type: ignore[arg-type]
        description="Control surface bind policy (loopback by default).",
    )
    logging: LoggingCfg = Field(
        default_factory=LoggingCfg,  # type: ignore[arg-type]
        description="Logging level configuration.",
    )


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load an :class:`AppConfig` from disk or build the default.

    When ``path`` is provided, the named YAML file is read and merged
    with built-in defaults. Environment-variable overlay applies in
    either case via Pydantic Settings.

    Args:
        path: Optional path to a YAML configuration file.

    Returns:
        A fully-validated :class:`AppConfig` instance.

    Raises:
        FileNotFoundError: when ``path`` is given but doesn't exist.
        ValueError: when the YAML root is not a mapping.
    """
    if path is None:
        logger.debug("No config path provided; building defaults.")
        return AppConfig()

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config file not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"config root must be a YAML mapping, got {type(raw).__name__}")

    # Environment variables still overlay after merging YAML - Pydantic
    # Settings' precedence is env > init kwargs > defaults. To preserve
    # that, we construct the AppConfig with YAML-derived overrides; env
    # then wins per Pydantic's normal Settings flow.
    overlay = _apply_env_overlay(raw)
    logger.debug("Loaded config from %s", p)
    return AppConfig.model_validate(overlay)


def _apply_env_overlay(raw: dict[str, Any]) -> dict[str, Any]:
    """Merge ``PHANTOM_EMULATOR_*`` env vars into the raw YAML mapping.

    This is a belt-and-braces overlay so callers can rely on
    :func:`load_config` honoring env vars even when the YAML path
    explicitly provides a value. Pydantic Settings already honors env
    vars on bare ``AppConfig()``; this helper applies the same merging
    rule when YAML is in play.

    Args:
        raw: The parsed YAML mapping.

    Returns:
        A new mapping with env-var overrides applied.
    """
    prefix = "PHANTOM_EMULATOR_"
    result: dict[str, Any] = dict(raw)
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        path_parts = key[len(prefix) :].lower().split("__")
        _set_nested(result, path_parts, value)
    return result


def _set_nested(d: dict[str, Any], parts: list[str], value: str) -> None:
    """Set a nested dict key from a flattened env-var path."""
    cursor: dict[str, Any] = d
    for part in parts[:-1]:
        nxt = cursor.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cursor[part] = nxt
        cursor = nxt
    cursor[parts[-1]] = value
