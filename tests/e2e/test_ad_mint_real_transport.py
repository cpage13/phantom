"""Autonomous AD client-credentials mint over the real transport (audit T3 / G2).

This is the regression guard for the AD-mint path. It drives the real
``AdMinter`` through the real ``azure.identity.aio.ClientSecretCredential``
against a temporary HTTPS OAuth authority, and requires the minted token to
land in a real ``TokenCache`` with ``source="plugin_mint"``.

It exists because the mint path had no end-to-end coverage: config and
supervision unit tests inject synthetic exceptions and never call the real
``_mint``, so a missing ``aiohttp`` (azure-identity's async transport
dependency, declared only as an optional extra) went unnoticed and would crash
every ad_mint instance at credential construction. This test constructs the
real async credential, so that regression fails here at the first mint.

Covered: real Azure credential construction, the OAuth token wire, the
primary-to-secondary secret fallback, the token-cache write, the fail-fast
double-failure path, and log hygiene (no secret or token bytes). Not covered
here (larger follow-up): parked-row wakeup and authenticated upstream delivery
through the full subprocess stack.
"""

from __future__ import annotations

import datetime as dt
import hmac
import ipaddress
import json
import os
import ssl
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from phantom.config.ad_mint import AdMintConfig
from phantom.refresh.ad_client_credentials import AdMinter, AuthUnavailableError
from phantom.storage.token_cache import SqliteTokenCache

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_TENANT_ID = "t3-tenant"
_CLIENT_ID = "t3-client"
_SCOPE = "api://phantom-t3/.default"
_ENDPOINT = "files.upstream.example"
_UID = "t3-ad-uid"
_GOOD_SECRET = "t3-secondary-secret-CORRECT"
_BAD_SECRET = "t3-primary-secret-WRONG"
_PRIMARY_ENV = "PHANTOM_T3_PRIMARY_SECRET"
_SECONDARY_ENV = "PHANTOM_T3_SECONDARY_SECRET"
# Distinctive so a cache assertion proves the stored bearer is exactly what the
# authority issued, not some other value.
_ISSUED_TOKEN = "t3-issued-access-token-8f2c1a"


def _write_temp_cert(directory: Path) -> tuple[Path, Path]:
    """Generate a self-signed cert/key for 127.0.0.1 and return their paths."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "authority-cert.pem"
    key_path = directory / "authority-key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


class _TenantOAuthStub:
    """Temporary HTTPS AAD-shaped token authority that records mint attempts.

    Validates the presented ``client_secret`` against ``expected_secret`` with
    ``hmac.compare_digest``; a match mints a fixed opaque token, a mismatch
    returns the AAD-shaped ``invalid_client`` 401. Records the ordered
    accepted/rejected outcome of each token POST. Never records the secret.
    """

    def __init__(self, cert_path: Path, key_path: Path, expected_secret: str) -> None:
        self._cert_path = cert_path
        self._key_path = key_path
        self._expected = expected_secret
        self.attempts_accepted: list[bool] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port = 0

    @property
    def authority_url(self) -> str:
        """The https authority origin azure-identity is pointed at."""
        return f"https://127.0.0.1:{self.port}"

    def start(self) -> None:
        stub = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b""
                form = parse_qs(raw.decode("utf-8"))
                presented = form.get("client_secret", [""])[0]
                accepted = hmac.compare_digest(presented, stub._expected)
                stub.attempts_accepted.append(accepted)
                if accepted:
                    body = json.dumps(
                        {
                            "access_token": _ISSUED_TOKEN,
                            "token_type": "Bearer",
                            "expires_in": 3600,
                        }
                    ).encode()
                    self.send_response(200)
                else:
                    body = json.dumps(
                        {
                            "error": "invalid_client",
                            "error_description": "credential rejected by t3 stub",
                        }
                    ).encode()
                    self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        server.daemon_threads = True
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self._cert_path), str(self._key_path))
        server.socket = context.wrap_socket(server.socket, server_side=True)
        self.port = int(server.server_address[1])
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)


@pytest.fixture(scope="module")
def trusted_authority_cert(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[Path, Path]]:
    """One stable cert for the whole module, trusted via ``SSL_CERT_FILE``.

    azure-identity's async transport caches its SSL context across credentials,
    so every mint in this module must present the SAME certificate. This also
    mirrors production, where the authority's CA is a single stable trust
    anchor in the system store, not a per-call certificate. The env is set once
    at module scope and restored at teardown.
    """
    cert_dir = tmp_path_factory.mktemp("t3-authority-cert")
    cert_path, key_path = _write_temp_cert(cert_dir)
    previous = {name: os.environ.get(name) for name in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")}
    os.environ["SSL_CERT_FILE"] = str(cert_path)
    os.environ["REQUESTS_CA_BUNDLE"] = str(cert_path)
    try:
        yield cert_path, key_path
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def _oauth_authority(
    cert: tuple[Path, Path], *, expected_secret: str
) -> Iterator[_TenantOAuthStub]:
    """Start an HTTPS OAuth authority using the module's trusted certificate."""
    cert_path, key_path = cert
    stub = _TenantOAuthStub(cert_path, key_path, expected_secret)
    stub.start()
    try:
        yield stub
    finally:
        stub.stop()


def _ad_config(authority_url: str, *, outage_retry: list[int]) -> AdMintConfig:
    """Build an AdMintConfig pointed at the temporary authority."""
    return AdMintConfig(
        tenant_id=_TENANT_ID,
        client_id=_CLIENT_ID,
        primary_client_secret_env=_PRIMARY_ENV,
        secondary_client_secret_env=_SECONDARY_ENV,
        authority_url=authority_url,
        scope=_SCOPE,
        refresh_seconds_before_expiry=60,
        refresh_jitter_seconds=0.0,
        ad_outage_retry_seconds=outage_retry,
        endpoint=_ENDPOINT,
        uid=_UID,
    )


async def test_primary_invalid_secondary_valid_mints_and_caches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    trusted_authority_cert: tuple[Path, Path],
) -> None:
    """Real transport: primary secret rejected, secondary mints into the cache."""
    with _oauth_authority(trusted_authority_cert, expected_secret=_GOOD_SECRET) as stub:
        monkeypatch.setenv(_PRIMARY_ENV, _BAD_SECRET)
        monkeypatch.setenv(_SECONDARY_ENV, _GOOD_SECRET)

        cache = SqliteTokenCache(str(tmp_path / "token_cache.db"))
        await cache.start()
        minter = AdMinter(config=_ad_config(stub.authority_url, outage_retry=[]), token_cache=cache)

        with caplog.at_level("DEBUG"):
            expiry = await minter._mint_and_store()

        # The real azure-identity client made two token POSTs: primary rejected,
        # secondary accepted. The fallback logic chose the working secret.
        assert stub.attempts_accepted == [False, True]
        assert expiry.tzinfo is not None

        row = await cache.get(_ENDPOINT, _UID)
        assert row is not None, "secondary mint did not write the (endpoint, uid) slot"
        assert row.source == "plugin_mint"
        assert row.bearer == f"Bearer {_ISSUED_TOKEN}"

        # Log hygiene: Phantom's own log records leak neither secret nor the
        # issued token. The scope is Phantom loggers deliberately. aiosqlite's
        # DEBUG statement log prints the bound bearer parameter on the cache
        # INSERT; that dependency-logger leak is real but is capped in
        # production by configure_logging (see ADR-004 / the T11 no-leak
        # guard), not something the minter controls. The minter's own contract
        # is that it never logs the secret or the token, which is what this
        # asserts.
        phantom_logs = "\n".join(
            record.getMessage() for record in caplog.records if record.name.startswith("phantom")
        )
        assert _GOOD_SECRET not in phantom_logs
        assert _BAD_SECRET not in phantom_logs
        assert _ISSUED_TOKEN not in phantom_logs


async def test_both_secrets_invalid_raises_auth_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trusted_authority_cert: tuple[Path, Path],
) -> None:
    """Real transport: both secrets rejected raises AuthUnavailableError, no cache write."""
    with _oauth_authority(trusted_authority_cert, expected_secret=_GOOD_SECRET) as stub:
        monkeypatch.setenv(_PRIMARY_ENV, _BAD_SECRET)
        monkeypatch.setenv(_SECONDARY_ENV, "t3-secondary-also-WRONG")

        cache = SqliteTokenCache(str(tmp_path / "token_cache.db"))
        await cache.start()
        minter = AdMinter(config=_ad_config(stub.authority_url, outage_retry=[]), token_cache=cache)

        with pytest.raises(AuthUnavailableError):
            await minter._mint_and_store()

        assert stub.attempts_accepted == [False, False]
        assert await cache.get(_ENDPOINT, _UID) is None


async def test_primary_valid_mints_in_one_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trusted_authority_cert: tuple[Path, Path],
) -> None:
    """Real transport: a valid primary secret mints in a single token POST."""
    with _oauth_authority(trusted_authority_cert, expected_secret=_GOOD_SECRET) as stub:
        monkeypatch.setenv(_PRIMARY_ENV, _GOOD_SECRET)
        monkeypatch.setenv(_SECONDARY_ENV, _BAD_SECRET)

        cache = SqliteTokenCache(str(tmp_path / "token_cache.db"))
        await cache.start()
        minter = AdMinter(config=_ad_config(stub.authority_url, outage_retry=[]), token_cache=cache)

        await minter._mint_and_store()

        assert stub.attempts_accepted == [True]
        row = await cache.get(_ENDPOINT, _UID)
        assert row is not None
        assert row.bearer == f"Bearer {_ISSUED_TOKEN}"
        assert row.source == "plugin_mint"
