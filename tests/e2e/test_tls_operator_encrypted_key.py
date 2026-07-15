"""Production CLI with an operator-supplied ENCRYPTED TLS key (audit T5 / G11).

The only TLS e2e on ``main`` (:mod:`tests.e2e.test_e2e_https_listener`) runs
the IN-PROCESS stack in auto-gen cert mode and reaches it with
``verify=False``; the TLS unit test self-builds ``uvicorn.Config`` and never
binds through ``python -m phantom``. Neither proves the production bootstrap
the audit named: the real CLI resolving an OPERATOR cert/key pair, passing
``key_password`` to uvicorn (``__main__.py``), completing TLS with the
password-decrypted key, and serving a real verified upload.

This module closes that gap over the real-subprocess lane:

* Happy path: an operator cert plus a password-ENCRYPTED PKCS8 key (owner-only
  perms) boot the real CLI with ``server.tls`` fully configured. Health, the
  SDK submission, and the admin poll all verify against the test CA (never
  ``verify=False``). The served peer certificate must be byte-identical (by
  SHA-256 fingerprint) to the configured operator cert, and the auto-gen pair
  must never be minted, so success is attributable to the operator branch.
  Delivery is proven at the emulator (byte-identical accepted body).
* Wrong-password negative: the same encrypted key with a wrong
  ``key_password`` must kill the process BEFORE health: non-zero exit, no
  listener ever bound, contextual error text, and NO password or key-PEM
  bytes anywhere in the complete child log.

Scope: this is the operator-encrypted-key arm of T5. Auto-gen cert lifecycle
(mint / reuse / rotation) stays owned by the TLS unit tests, and the
upload-over-TLS auto-gen path by ``test_e2e_https_listener``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import ipaddress
import os
import socket
import ssl
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    HOST,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    submit_one,
    write_phantom_config,
)
from tests.e2e.helpers.assertions import assert_chain_reaches_state

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

# Distinctive password sentinels: the no-leak scan proves NEITHER ever reaches
# the child log, so the values only need to be greppable, not secret.
_KEY_PASSWORD = "t5-operator-key-passphrase-c8f41a"
_WRONG_PASSWORD = "t5-wrong-key-passphrase-e2d907"
# High bytes included so the delivered body also exercises byte-transparency.
_PAYLOAD = b"phantom-t5-operator-tls-body\x00\xff\xfe-byte-identity"
# Budget for the buffered chain to reach terminal success after submission.
_SUCCEEDED_BUDGET_SECONDS = 20.0
# Budget for the wrong-password child to die at TLS bind (it never serves).
_EXIT_BUDGET_SECONDS = 30.0
# Owner-only mode for the encrypted key file (the audit's perms requirement).
_KEY_FILE_MODE = 0o600


def _write_operator_cert_with_encrypted_key(directory: Path, *, password: str) -> tuple[Path, Path]:
    """Generate an operator cert plus a password-ENCRYPTED PKCS8 key.

    Mirrors the T3 cert helper (``test_ad_mint_real_transport._write_temp_cert``)
    with the one load-bearing difference: the private key PEM is encrypted with
    ``BestAvailableEncryption(password)``, so uvicorn can only load it when the
    CLI hands over the correct ``server.tls.key_password``. The key file is
    written owner-only.

    Args:
        directory: Existing directory to write ``operator-cert.pem`` and
            ``operator-key.pem`` into.
        password: The passphrase the key PEM is encrypted with.

    Returns:
        ``(cert_path, key_path)`` of the written PEM pair.
    """
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
            x509.SubjectAlternativeName(
                [
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    x509.DNSName("localhost"),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "operator-cert.pem"
    key_path = directory / "operator-key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(password.encode()),
        )
    )
    os.chmod(key_path, _KEY_FILE_MODE)
    return cert_path, key_path


def _tls_config_overrides(cert_path: Path, key_path: Path, key_password: str) -> dict[str, object]:
    """The ``server.tls`` operator-supplied overlay for the pinned base config."""
    return {
        "server": {
            "tls": {
                "enabled": True,
                "cert_path": str(cert_path),
                "key_path": str(key_path),
                "key_password": key_password,
            }
        }
    }


def _served_certificate_der(host: str, port: int, ca_path: Path) -> bytes:
    """Complete a VERIFIED TLS handshake and return the peer cert's DER bytes.

    Trust is pinned to ``ca_path`` with hostname checking on (the operator
    cert carries an ``127.0.0.1`` IP SAN), so the handshake itself is already
    a verification proof; the returned DER feeds the fingerprint comparison.
    """
    context = ssl.create_default_context(cafile=str(ca_path))
    with (
        socket.create_connection((host, port), timeout=5.0) as raw_sock,
        context.wrap_socket(raw_sock, server_hostname=host) as tls_sock,
    ):
        der = tls_sock.getpeercert(binary_form=True)
    assert der is not None, "verified TLS handshake returned no peer certificate"
    return der


async def test_operator_encrypted_key_serves_verified_https_and_delivers(
    tmp_path: Path,
) -> None:
    """The real CLI decrypts the operator key, serves verified TLS, delivers.

    Objective: prove ``python -m phantom`` resolves the operator cert/key,
    passes ``key_password`` through to uvicorn, completes verified TLS on the
    single listener, and serves a real upload end to end. Success requires the
    served certificate to BE the configured operator cert (fingerprint match,
    auto-gen never minted) and the emulator to accept the byte-identical body.
    """
    data_dir = tmp_path / "data"
    operator_tls_dir = tmp_path / "operator-tls"
    operator_tls_dir.mkdir()
    cert_path, key_path = _write_operator_cert_with_encrypted_key(
        operator_tls_dir, password=_KEY_PASSWORD
    )

    emulator = await boot_emulator()
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_tls_config_overrides(cert_path, key_path, _KEY_PASSWORD),
    )
    proc = PhantomSubprocess.make(config_path, port, tls_verify=str(cert_path))
    try:
        # start() health-polls https with verify pinned to the operator cert:
        # a listener that failed to decrypt the key (or serves any other cert)
        # can never pass this gate.
        await proc.start()
        assert proc.url.startswith("https://"), (
            f"tls_verify harness lane must produce an https url; got {proc.url!r}"
        )

        # The served peer certificate is EXACTLY the configured operator cert.
        served_der = _served_certificate_der(HOST, port, cert_path)
        configured_der = x509.load_pem_x509_certificate(cert_path.read_bytes()).public_bytes(
            serialization.Encoding.DER
        )
        assert hashlib.sha256(served_der).hexdigest() == (
            hashlib.sha256(configured_der).hexdigest()
        ), "listener served a certificate other than the configured operator cert"

        # The operator branch never mints the auto-gen pair (resolve_tls_paths
        # returns the supplied paths verbatim; <data_dir>/tls/ must not exist).
        assert not (data_dir / "tls").exists(), (
            "auto-gen TLS pair was minted despite operator-supplied cert/key"
        )

        # Verified SDK submission over the TLS listener (never verify=False).
        bearer = fake_security_token(emulator)
        chain_id = uuid4()
        transport = httpx.AsyncHTTPTransport(verify=str(cert_path))
        async with PhantomClient(proc.url, transport=transport) as client:
            await submit_one(
                client,
                emulator_url=emulator.url,
                bearer=bearer,
                body=_PAYLOAD,
                chain_id=chain_id,
            )
            detail = await assert_chain_reaches_state(
                client,
                chain_id,
                state="succeeded",
                timeout_seconds=_SUCCEEDED_BUDGET_SECONDS,
            )
        assert detail.state == "succeeded"

        # Upstream delivery: the emulator accepted exactly one body, OURS
        # (correlated by phantom_local_uuid), byte-identical through the TLS
        # ingress (ReceivedEntry.body_hash is the sink-side SHA-256 of the raw
        # PUT bytes — the transparent-proxy oracle).
        received = emulator.received()
        assert len(received) == 1, (
            f"expected exactly one accepted upstream body, got {len(received)}"
        )
        entry = received[0]
        assert entry.metadata_kvs.get("phantom_local_uuid") == str(chain_id), (
            "accepted upstream body does not correlate to the submitted chain"
        )
        assert entry.body_hash == hashlib.sha256(_PAYLOAD).hexdigest(), (
            "byte round-trip broke over the operator-TLS listener"
        )
    finally:
        proc.terminate()
        await emulator.stop()


async def test_wrong_key_password_exits_before_health_with_no_leak(
    tmp_path: Path,
) -> None:
    """A wrong ``key_password`` kills the CLI at bind, leaking nothing.

    Objective: the strict negative from the audit. The process must exit
    non-zero BEFORE ever serving (no listener bound), the log must carry
    contextual TLS-key error text, and the complete log must contain neither
    password sentinel nor any line of the encrypted key PEM.
    """
    data_dir = tmp_path / "data"
    operator_tls_dir = tmp_path / "operator-tls"
    operator_tls_dir.mkdir()
    cert_path, key_path = _write_operator_cert_with_encrypted_key(
        operator_tls_dir, password=_KEY_PASSWORD
    )

    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_tls_config_overrides(cert_path, key_path, _WRONG_PASSWORD),
    )
    proc = PhantomSubprocess.make(config_path, port, tls_verify=str(cert_path))
    try:
        # spawn() (not start()): the wrong-password child must die BEFORE
        # health, so the health poll would misreport the expected exit.
        proc.spawn(label=f"t5-no-health-wait config={proc.config_path}")
        returncode = await proc.wait_for_expected_exit(timeout_seconds=_EXIT_BUDGET_SECONDS)
        assert returncode != 0

        log_text = proc.read_full_log()

        # The listener never came up: uvicorn's post-bind banner is absent
        # from the log, and the port refuses connections post-mortem.
        assert "Uvicorn running on" not in log_text, (
            "wrong-password child bound a listener before dying"
        )
        with pytest.raises(OSError), socket.create_connection((HOST, port), timeout=2.0):
            pass

        # Contextual error text: the operator can tell WHY it died. The
        # observed death is uvicorn's create_ssl_context -> load_cert_chain
        # raising ssl.SSLError on the undecryptable key (empirically pinned;
        # the OpenSSL "[SSL] PEM lib (_ssl.c:NNNN)" detail line is
        # interpreter-specific and deliberately NOT asserted).
        assert "ssl.SSLError" in log_text, (
            "child log carries no ssl.SSLError context for the TLS key-load failure"
        )
        assert "load_cert_chain" in log_text, (
            "child log does not attribute the failure to certificate/key loading"
        )

        # No-leak scan over the COMPLETE log: neither password sentinel and
        # no line of the encrypted key PEM (header or base64 body).
        assert _WRONG_PASSWORD not in log_text, "configured key password leaked into the log"
        assert _KEY_PASSWORD not in log_text, "correct key password leaked into the log"
        assert "ENCRYPTED PRIVATE KEY" not in log_text, "key PEM header leaked into the log"
        for pem_line in key_path.read_text().splitlines():
            if not pem_line or pem_line.startswith("-----"):
                continue
            assert pem_line not in log_text, "encrypted key PEM content leaked into the log"
    finally:
        proc.terminate()
