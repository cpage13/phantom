"""Phase 5 — the single in-code TLS (HTTPS) listener + cert lifecycle.

Covers the whole Phase-5 gate in one module:

* the REAL in-process HTTPS boot: Phantom's ONE listener served with
  ``tls.enabled`` + a generated cert returns 200 from ``GET /v1/healthz`` over
  ``https://`` with ``verify=False``, and a plaintext ``http://`` GET to that
  same port fails (HTTPS-only — proof no second/plaintext listener leaked);
* the ``ssl_*`` splat into the EXISTING ``uvicorn.run`` (a kwargs-RECORDING spy,
  NOT the raising ``_explode``): enabled splats the cert/key (and the password
  only for an operator-supplied pair); off leaves the call byte-for-byte
  plaintext (no ``ssl_*`` keys);
* the cert auto-gen (valid self-signed CN/SAN + validity window + key ``0o600``);
* rotation: a missing OR expired/near-expiry cert regenerates; a present-and-
  valid one is reused;
* the ``TlsCfg`` XOR validator rejects the half-configured (exactly-one-path)
  state.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import ssl
import stat
from pathlib import Path

import httpx
import pytest
import uvicorn
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from phantom.__main__ import main
from phantom.app import create_app
from phantom.config.settings import (
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
    TlsCfg,
)
from phantom.runtime.tls_cert import (
    CERT_VALIDITY,
    RENEWAL_SKEW,
    TlsCertError,
    resolve_tls_paths,
)
from pydantic import ValidationError

_LOOPBACK = "127.0.0.1"
# Generous startup ceiling for the in-process boot (mirrors the e2e helper's
# 15s budget); the poll interval keeps the wait tight in the happy path.
_BOOT_TIMEOUT_SECONDS: float = 15.0
_BOOT_POLL_SECONDS: float = 0.02
# Short client timeout so the plaintext-against-TLS negative control fails fast
# (a non-raising hang is a failure, not a stall).
_CLIENT_TIMEOUT_SECONDS: float = 2.0


def _allocate_port() -> int:
    """Bind an ephemeral loopback port, then release it for uvicorn to claim."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((_LOOPBACK, 0))
        return int(probe.getsockname()[1])
    finally:
        probe.close()


def _one_instance_settings(tmp_path: Path, tls: TlsCfg) -> Settings:
    """A minimal-but-real Settings with one instance + the given TLS block."""
    return Settings(
        storage=StorageCfg(data_dir=str(tmp_path)),
        server={"bind_tcp": f"{_LOOPBACK}:0", "tls": tls},  # type: ignore[arg-type]
        instances=[
            InstanceCfg(
                id="primary",
                host_prefixes=["upstream.example.com"],
                data_dir="primary",
                routes=[
                    RouteCfg(
                        name="upstream-files",
                        hosts=["upstream.example.com"],
                        auth_mode="phantom_bearer",
                    )
                ],
            )
        ],
    )


def _write_min_yaml(path: Path, *, tls: dict[str, object] | None) -> None:
    """Write a minimal valid Phantom YAML, optionally with a server.tls block."""
    doc: dict[str, object] = {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["upstream.example.com"],
                "data_dir": "primary",
                "routes": [
                    {
                        "name": "upstream-files",
                        "hosts": ["upstream.example.com"],
                        "auth_mode": "phantom_bearer",
                    }
                ],
            }
        ],
    }
    if tls is not None:
        doc["server"] = {"tls": tls}
    path.write_text(yaml.safe_dump(doc))


# ---------------------------------------------------------------------------
# The TLS-listener boot test (the load-bearing one — proves real HTTPS).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_listener_serves_https_200(tmp_path: Path) -> None:
    """The ONE listener, served with TLS, answers /v1/healthz 200 over https.

    Boots Phantom's single listener in-process with ``tls.enabled`` + an
    auto-generated cert, then an ``httpx`` ``verify=False`` client gets 200
    from ``GET /v1/healthz`` on the encrypted wire. The negative control proves
    a plaintext ``http://`` GET to the SAME port fails — the socket is TLS-only,
    so Phase 5 did not leave a plaintext path or add a second listener.
    """
    cert_path, key_path = resolve_tls_paths(TlsCfg(enabled=True), str(tmp_path))
    settings = _one_instance_settings(tmp_path, TlsCfg(enabled=True))
    app = create_app(settings)
    port = _allocate_port()

    config = uvicorn.Config(
        app=app,
        host=_LOOPBACK,
        port=port,
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
        lifespan="on",
        access_log=False,
        log_level="warning",
    )
    server = uvicorn.Server(config=config)
    serve_task = asyncio.create_task(server.serve())
    try:
        deadline = asyncio.get_event_loop().time() + _BOOT_TIMEOUT_SECONDS
        while not server.started:
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("TLS listener failed to start in time")
            await asyncio.sleep(_BOOT_POLL_SECONDS)

        url = f"https://{_LOOPBACK}:{port}/v1/healthz"
        async with httpx.AsyncClient(verify=False, timeout=_CLIENT_TIMEOUT_SECONDS) as client:
            resp = await client.get(url)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # Negative control: plaintext http:// to the TLS-only port must fail.
        # Keep the matcher BROAD — a plaintext request against a TLS socket
        # surfaces variably (ConnectError / RemoteProtocolError, both under
        # TransportError); the point is "no plaintext path leaked," not which
        # precise error TLS rejection produces.
        with pytest.raises((httpx.TransportError, httpx.RemoteProtocolError)):
            async with httpx.AsyncClient(timeout=_CLIENT_TIMEOUT_SECONDS) as plain:
                await plain.get(f"http://{_LOOPBACK}:{port}/v1/healthz")
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve_task, timeout=_BOOT_TIMEOUT_SECONDS)


# ---------------------------------------------------------------------------
# The ssl_* splat into the EXISTING uvicorn.run (recording spy, TASK 5.1).
# ---------------------------------------------------------------------------


def test_tls_off_leaves_uvicorn_run_plaintext(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With TLS off (the default), uvicorn.run carries NO ssl_* kwargs.

    Drives the NON-``--validate`` path, so the serve block runs and
    ``uvicorn.run`` IS called — hence a kwargs-RECORDING spy that returns None
    (contrast ``test_validate_flag_does_not_bind_server``'s raising sentinel).
    """
    cfg = tmp_path / "phantom.yaml"
    _write_min_yaml(cfg, tls=None)  # no server.tls block at all → off by default
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg)])

    calls: list[dict[str, object]] = []

    def _record(*_args: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(uvicorn, "run", _record)
    exit_code = main()
    assert exit_code == 0
    assert len(calls) == 1
    recorded = calls[0]
    assert "ssl_certfile" not in recorded
    assert "ssl_keyfile" not in recorded
    assert "ssl_keyfile_password" not in recorded


def test_tls_enabled_splats_ssl_kwargs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With operator-supplied cert/key + a password, the ssl_* kwargs reach uvicorn.run."""
    # Mint an operator-supplied pair on disk (paths set → operator branch).
    cert_path, key_path = resolve_tls_paths(TlsCfg(enabled=True), str(tmp_path / "gen"))

    cfg = tmp_path / "phantom.yaml"
    _write_min_yaml(
        cfg,
        tls={
            "enabled": True,
            "cert_path": cert_path,
            "key_path": key_path,
            "key_password": "s3cret",
        },
    )
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg)])

    calls: list[dict[str, object]] = []

    def _record(*_args: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(uvicorn, "run", _record)
    exit_code = main()
    assert exit_code == 0
    assert len(calls) == 1
    recorded = calls[0]
    assert recorded["ssl_certfile"] == cert_path
    assert recorded["ssl_keyfile"] == key_path
    # Password is gated on the operator-supplied case (cert_path is not None).
    assert recorded["ssl_keyfile_password"] == "s3cret"


def test_tls_autogen_omits_keyfile_password(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-gen (both paths None) NEVER passes ssl_keyfile_password.

    Even with ``key_password`` set, an auto-gen config (paths None) writes the
    key UNENCRYPTED, so the splat must NOT forward the password (else uvicorn
    would try to decrypt an unencrypted key and fail at bind). The cert/key it
    splats are the freshly-minted auto-gen paths.
    """
    # storage.data_dir under tmp_path so the auto-gen writes there, not the
    # /var/lib/phantom default.
    cfg = tmp_path / "phantom.yaml"
    _write_min_yaml(cfg, tls={"enabled": True, "key_password": "ignored"})
    cfg_doc = yaml.safe_load(cfg.read_text())
    cfg_doc["storage"] = {"data_dir": str(tmp_path / "data")}
    cfg.write_text(yaml.safe_dump(cfg_doc))
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg)])

    calls: list[dict[str, object]] = []

    def _record(*_args: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(uvicorn, "run", _record)
    exit_code = main()
    assert exit_code == 0
    assert len(calls) == 1
    recorded = calls[0]
    assert "ssl_certfile" in recorded
    assert "ssl_keyfile" in recorded
    assert "ssl_keyfile_password" not in recorded


# ---------------------------------------------------------------------------
# Cert auto-gen (TASK 5.2).
# ---------------------------------------------------------------------------


def test_autogen_produces_valid_self_signed_cert(tmp_path: Path) -> None:
    """Auto-gen mints a valid self-signed cert: CN/SAN, validity window, ca=False, key 0o600."""
    cert_path_str, key_path_str = resolve_tls_paths(TlsCfg(enabled=True), str(tmp_path))
    cert_path = Path(cert_path_str)
    key_path = Path(key_path_str)
    assert cert_path.is_file()
    assert key_path.is_file()

    cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
    # CN == localhost.
    cns = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    assert [attr.value for attr in cns] == ["localhost"]
    # SAN = {DNS localhost, IP 127.0.0.1}.
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.DNSName) == ["localhost"]
    assert san.get_values_for_type(x509.IPAddress) == [ipaddress.IPv4Address("127.0.0.1")]
    # Self-signed: issuer == subject.
    assert cert.issuer == cert.subject
    # Validity window == CERT_VALIDITY (tz-aware accessors).
    assert cert.not_valid_after_utc - cert.not_valid_before_utc == CERT_VALIDITY
    assert cert.not_valid_after_utc > datetime.datetime.now(tz=datetime.UTC)
    # Not a CA.
    bc = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
    assert bc.ca is False

    # Key PEM is owner-only (0o600).
    mode = stat.S_IMODE(key_path.stat().st_mode)
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    # The pair actually loads into an SSLContext (proves cert/key match + parse).
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=cert_path_str, keyfile=key_path_str)


# ---------------------------------------------------------------------------
# Rotation / reuse (TASK 5.2).
# ---------------------------------------------------------------------------


def test_reuse_when_present_and_valid(tmp_path: Path) -> None:
    """A present-and-valid pair is REUSED (same serial) on a second resolve."""
    cfg = TlsCfg(enabled=True)
    cert_path_str, _ = resolve_tls_paths(cfg, str(tmp_path))
    serial_first = x509.load_pem_x509_certificate(Path(cert_path_str).read_bytes()).serial_number

    cert_path_str2, _ = resolve_tls_paths(cfg, str(tmp_path))
    serial_second = x509.load_pem_x509_certificate(Path(cert_path_str2).read_bytes()).serial_number

    assert cert_path_str == cert_path_str2
    assert serial_first == serial_second  # not regenerated


def test_rotation_on_missing_cert(tmp_path: Path) -> None:
    """A missing cert (empty data dir) triggers generation."""
    cert_path_str, key_path_str = resolve_tls_paths(TlsCfg(enabled=True), str(tmp_path))
    assert Path(cert_path_str).is_file()
    assert Path(key_path_str).is_file()


def test_rotation_on_expired_cert(tmp_path: Path) -> None:
    """An expired existing cert is REGENERATED (new serial, fresh future expiry).

    Generate a valid pair first, then overwrite the cert file with a freshly
    minted but ALREADY-EXPIRED self-signed cert at the same stable path; the
    next resolve must detect the past ``not_valid_after_utc`` and mint anew.
    """
    cfg = TlsCfg(enabled=True)
    cert_path_str, _ = resolve_tls_paths(cfg, str(tmp_path))
    cert_path = Path(cert_path_str)
    serial_before = x509.load_pem_x509_certificate(cert_path.read_bytes()).serial_number

    # Overwrite with an expired cert (not_valid_after in the past).
    expired_pem = _mint_expired_cert_pem()
    cert_path.write_bytes(expired_pem)
    loaded_expired = x509.load_pem_x509_certificate(cert_path.read_bytes())
    assert loaded_expired.not_valid_after_utc < datetime.datetime.now(tz=datetime.UTC)

    cert_path_str2, _ = resolve_tls_paths(cfg, str(tmp_path))
    regenerated = x509.load_pem_x509_certificate(Path(cert_path_str2).read_bytes())
    assert regenerated.serial_number != serial_before
    assert regenerated.serial_number != loaded_expired.serial_number
    # Fresh cert is valid well beyond the renewal skew.
    now = datetime.datetime.now(tz=datetime.UTC)
    assert regenerated.not_valid_after_utc > now + RENEWAL_SKEW


def test_rotation_on_near_expiry_cert(tmp_path: Path) -> None:
    """A cert expiring WITHIN the renewal skew is regenerated (rotate-before-lapse)."""
    cfg = TlsCfg(enabled=True)
    cert_path_str, _ = resolve_tls_paths(cfg, str(tmp_path))
    cert_path = Path(cert_path_str)
    serial_before = x509.load_pem_x509_certificate(cert_path.read_bytes()).serial_number

    # A cert valid, but expiring 1 day out — inside RENEWAL_SKEW (7d).
    near_pem = _mint_cert_pem(valid_for=datetime.timedelta(days=1))
    cert_path.write_bytes(near_pem)

    cert_path_str2, _ = resolve_tls_paths(cfg, str(tmp_path))
    regenerated = x509.load_pem_x509_certificate(Path(cert_path_str2).read_bytes())
    assert regenerated.serial_number != serial_before


def test_rotation_on_corrupt_cert(tmp_path: Path) -> None:
    """An unparseable existing cert is regenerated (not a hard failure)."""
    cfg = TlsCfg(enabled=True)
    cert_path_str, _ = resolve_tls_paths(cfg, str(tmp_path))
    cert_path = Path(cert_path_str)
    serial_before = x509.load_pem_x509_certificate(cert_path.read_bytes()).serial_number

    cert_path.write_bytes(b"-----BEGIN CERTIFICATE-----\nnot a real cert\n")

    cert_path_str2, _ = resolve_tls_paths(cfg, str(tmp_path))
    regenerated = x509.load_pem_x509_certificate(Path(cert_path_str2).read_bytes())
    assert regenerated.serial_number != serial_before


# ---------------------------------------------------------------------------
# Operator-supplied path validation (TASK 5.2).
# ---------------------------------------------------------------------------


def test_operator_supplied_missing_path_raises(tmp_path: Path) -> None:
    """An operator-supplied cert_path that does not exist fails fast with TlsCertError."""
    missing_cert = str(tmp_path / "nope.crt")
    missing_key = str(tmp_path / "nope.key")
    cfg = TlsCfg(enabled=True, cert_path=missing_cert, key_path=missing_key)
    with pytest.raises(TlsCertError):
        resolve_tls_paths(cfg, str(tmp_path))


def test_operator_supplied_present_paths_returned_verbatim(tmp_path: Path) -> None:
    """Operator-supplied present paths are returned unchanged (never auto-rotated)."""
    # Reuse the auto-gen to produce a real pair, then feed those paths as operator-supplied.
    gen_cert, gen_key = resolve_tls_paths(TlsCfg(enabled=True), str(tmp_path / "gen"))
    cfg = TlsCfg(enabled=True, cert_path=gen_cert, key_path=gen_key)
    out_cert, out_key = resolve_tls_paths(cfg, str(tmp_path))
    assert out_cert == gen_cert
    assert out_key == gen_key


# ---------------------------------------------------------------------------
# The TlsCfg XOR validator (TASK 5.1).
# ---------------------------------------------------------------------------


def test_half_configured_cert_only_rejected() -> None:
    """enabled + cert_path set but key_path None is rejected (half-configured)."""
    with pytest.raises(ValidationError):
        TlsCfg(enabled=True, cert_path="/tmp/cert.pem", key_path=None)


def test_half_configured_key_only_rejected() -> None:
    """enabled + key_path set but cert_path None is rejected (half-configured)."""
    with pytest.raises(ValidationError):
        TlsCfg(enabled=True, cert_path=None, key_path="/tmp/key.pem")


def test_both_none_when_enabled_is_valid() -> None:
    """enabled + both paths None is VALID (means auto-gen)."""
    cfg = TlsCfg(enabled=True)
    assert cfg.enabled is True
    assert cfg.cert_path is None
    assert cfg.key_path is None


def test_both_set_when_enabled_is_valid() -> None:
    """enabled + both paths set is VALID (operator-supplied)."""
    cfg = TlsCfg(enabled=True, cert_path="/tmp/c.pem", key_path="/tmp/k.pem")
    assert cfg.cert_path == "/tmp/c.pem"
    assert cfg.key_path == "/tmp/k.pem"


def test_half_configured_allowed_when_disabled() -> None:
    """When disabled, the XOR guard does NOT apply (paths are inert)."""
    cfg = TlsCfg(enabled=False, cert_path="/tmp/c.pem", key_path=None)
    assert cfg.enabled is False
    assert cfg.cert_path == "/tmp/c.pem"


def test_default_tls_is_disabled() -> None:
    """A default ServerCfg().tls is disabled (zero behavior change for existing configs)."""
    settings = Settings()
    assert settings.server.tls.enabled is False
    assert settings.server.tls.cert_path is None


# ---------------------------------------------------------------------------
# Test helpers — mint certs with a chosen validity for the rotation tests.
# ---------------------------------------------------------------------------


def _mint_cert_pem(*, valid_for: datetime.timedelta) -> bytes:
    """Mint a self-signed cert PEM expiring ``valid_for`` from now."""
    now = datetime.datetime.now(tz=datetime.UTC)
    return _mint_cert_pem_between(not_before=now, not_after=now + valid_for)


def _mint_expired_cert_pem() -> bytes:
    """Mint a self-signed cert PEM whose validity window is entirely in the past."""
    now = datetime.datetime.now(tz=datetime.UTC)
    return _mint_cert_pem_between(
        not_before=now - datetime.timedelta(days=2),
        not_after=now - datetime.timedelta(days=1),
    )


def _mint_cert_pem_between(
    *,
    not_before: datetime.datetime,
    not_after: datetime.datetime,
) -> bytes:
    """Mint a self-signed cert PEM with an explicit validity window."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM)
