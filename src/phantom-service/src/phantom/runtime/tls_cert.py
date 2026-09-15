"""Startup TLS certificate lifecycle for the single in-code HTTPS listener.

Phantom's listener can speak HTTPS when ``server.tls.enabled`` is set
(``__main__.py``): the launcher passes ``ssl_certfile``/``ssl_keyfile`` to the
EXISTING ``uvicorn.run`` (``uvicorn/main.py:534-536``). This module supplies
that PEM pair through one public entry point, :func:`resolve_tls_paths`, with
two modes decided by whether the operator supplied paths:

* **Operator-supplied** - both ``cert_path`` and ``key_path`` set: validated to
  exist and returned verbatim. Phantom never regenerates an operator's cert
  (their lifecycle, their problem).
* **Auto-gen** (the owner default) - both paths None: mint a self-signed cert
  (CN/SAN ``localhost`` + ``127.0.0.1``) at a STABLE path under the data dir,
  reusing a present-and-valid pair and REGENERATING a missing / expired /
  near-expiry one. This is STARTUP-time rotation: the launcher calls this once
  before ``uvicorn.run`` binds, so "regenerate on/near expiry" fires on each
  process start. The :class:`phantom.config.settings.TlsCfg` XOR validator
  guarantees both-or-neither, so these two modes are exhaustive.

Auto-gen keys are written UNENCRYPTED (``NoEncryption()``) at ``0o600`` on the
same box as the producer - a passphrase adds nothing, and the launcher passes
``ssl_keyfile_password`` ONLY for the operator-supplied case, so an auto-gen key
MUST be unencrypted or uvicorn would later try to decrypt it with no password.

Phantom never VERIFIES this cert - the trust model is "encrypt the local hop",
not CA identity (clients use ``verify=off`` or pin the generated cert).

**Out of scope (future work):** ``certbot`` / ACME for properly-signed certs
when online (localhost has no domain, so self-signed is the norm); in-process
LIVE rotation (regenerating the ``SSLContext`` mid-serve without a restart -
uvicorn binds the context once at ``Server.startup``). Startup-time
regenerate-on-expiry covers the owner requirement for a restart-on-deploy
sidecar.
"""

from __future__ import annotations

import datetime
import ipaddress
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.types import PublicKeyTypes

    from phantom.config.settings import TlsCfg


class _HasPublicKey(Protocol):
    """Anything that can hand over its public half.

    Both :class:`x509.Certificate` and every loaded private-key type satisfy
    this, which is what lets one helper serialize either side of the pair
    for comparison.
    """

    def public_key(self) -> PublicKeyTypes:
        """Return the public half."""
        ...  # pragma: no cover - Protocol stub.


logger = logging.getLogger(__name__)


# Validity window for a freshly-minted self-signed cert. 825 days matches the
# operator one-liner the design documents (`openssl req ... -days 825`) and the
# CA/Browser-Forum max for non-public certs; it is a fixed minting parameter,
# not a per-deploy knob (an operator who wants control supplies their own cert).
CERT_VALIDITY: datetime.timedelta = datetime.timedelta(days=825)
# Regenerate when the existing cert expires within this skew of "now", so a
# long-lived sidecar rotates BEFORE the cert lapses rather than at the lapse.
# Fixed minting parameter (see CERT_VALIDITY).
RENEWAL_SKEW: datetime.timedelta = datetime.timedelta(days=7)

# RSA key size for auto-gen. 2048 mirrors the operator `openssl req -newkey
# rsa:2048` one-liner; sufficient for a loopback self-signed cert.
_RSA_KEY_SIZE: int = 2048
# Standard RSA public exponent (F4 / 65537).
_RSA_PUBLIC_EXPONENT: int = 65537

# Stable filenames for the auto-gen pair under ``<data_dir>/tls/``.
_AUTOGEN_DIRNAME: str = "tls"
_AUTOGEN_CERT_NAME: str = "phantom-self-signed.crt"
_AUTOGEN_KEY_NAME: str = "phantom-self-signed.key"

# Owner-only perms for the private key file and the cert directory, consistent
# with the process umask hardening (``startup_checks.apply_umask`` /
# ``UMASK_OWNER_ONLY``). The cert PEM is non-secret but lives in the same dir.
_KEY_FILE_MODE: int = 0o600
_TLS_DIR_MODE: int = 0o700


class TlsCertError(RuntimeError):
    """A TLS cert could not be resolved (missing operator path, or gen failure).

    Raised on a missing/unreadable operator-supplied path (fail fast, friendlier
    than uvicorn's later ``load_cert_chain``) and on an unrecoverable
    generation/write failure. Mirrors the ``runtime/startup_checks.py``
    custom-exception style (``IntegrityFailClosedError``).
    """


def resolve_tls_paths(tls: TlsCfg, data_dir: str) -> tuple[str, str]:
    """Resolve a :class:`TlsCfg` to a usable ``(cert_path, key_path)`` PEM pair.

    Args:
        tls: The validated ``server.tls`` config. Its XOR validator guarantees
            ``cert_path``/``key_path`` are both-set (operator-supplied) or
            both-None (auto-gen); this function relies on that exhaustiveness.
        data_dir: ``storage.data_dir`` - the root under which the auto-gen pair
            is minted/reused at ``<data_dir>/tls/`` (stable across restarts).

    Returns:
        ``(cert_path, key_path)`` as filesystem path strings ready to hand to
        ``uvicorn.run``'s ``ssl_certfile``/``ssl_keyfile``.

    Raises:
        TlsCertError: An operator-supplied path is missing/unreadable, or
            auto-generation failed.
    """
    if tls.cert_path is not None and tls.key_path is not None:
        return _validate_operator_paths(tls.cert_path, tls.key_path)
    # Both-None (the validator forbids exactly-one): auto-gen / reuse / rotate.
    return _resolve_autogen(data_dir)


def _validate_operator_paths(cert_path: str, key_path: str) -> tuple[str, str]:
    """Confirm operator-supplied cert + key files exist and are readable."""
    for label, path_str in (("cert_path", cert_path), ("key_path", key_path)):
        path = Path(path_str)
        if not path.is_file():
            raise TlsCertError(
                f"server.tls.{label} {path_str!r} is not a readable file "
                "(operator-supplied cert/key must exist before startup)"
            )
        if not os.access(path, os.R_OK):
            raise TlsCertError(
                f"server.tls.{label} {path_str!r} exists but is not readable "
                "(check file permissions)"
            )
    return cert_path, key_path


def _resolve_autogen(data_dir: str) -> tuple[str, str]:
    """Mint / reuse / rotate the self-signed pair under ``<data_dir>/tls/``.

    Generates when EITHER half of the pair is missing, unparseable or
    mismatched, or when the cert is expired / within :data:`RENEWAL_SKEW` of
    expiry; otherwise reuses the present-and-valid pair.
    """
    tls_dir = Path(data_dir) / _AUTOGEN_DIRNAME
    cert_path = tls_dir / _AUTOGEN_CERT_NAME
    key_path = tls_dir / _AUTOGEN_KEY_NAME

    if _needs_regeneration(cert_path, key_path):
        _generate_self_signed(tls_dir, cert_path, key_path)
    return str(cert_path), str(key_path)


def _public_key_der(key: _HasPublicKey) -> bytes:
    """Return a key holder's public half as DER SubjectPublicKeyInfo bytes.

    The comparable form: two objects describe the same key pair iff their
    SubjectPublicKeyInfo encodings are byte-equal. Serializing rather than
    comparing key objects keeps the check algorithm-agnostic, so a
    hand-placed EC or Ed25519 pair is judged as correctly as the RSA pair
    this module mints.

    Args:
        key: Anything exposing ``public_key()`` - a certificate or a loaded
            private key.

    Returns:
        The DER-encoded SubjectPublicKeyInfo.
    """
    return key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _needs_regeneration(cert_path: Path, key_path: Path) -> bool:
    """Return whether the auto-gen PAIR must be re-minted.

    Both halves are inspected, because uvicorn needs both and it loads them
    long after this module has decided the pair is fine. Checking only the
    certificate meant a valid cert beside a missing, unreadable or
    MISMATCHED key was returned unchanged, and the process then died inside
    ``load_cert_chain`` with a ``FileNotFoundError`` or an ``ssl.SSLError``
    - after ``create_app`` had already logged a healthy factory. Since a
    minted cert stays valid for :data:`CERT_VALIDITY` (825 days), every
    restart took that identical branch: a self-healing path that could not
    heal the half that was actually broken.

    Reuse only when: the cert loads cleanly, its tz-aware
    ``not_valid_after_utc`` is more than :data:`RENEWAL_SKEW` in the future,
    the key loads cleanly, and the key's public half matches the cert's.

    Args:
        cert_path: The auto-gen certificate path.
        key_path: The auto-gen private-key path (unencrypted by design; see
            the module docstring).

    Returns:
        ``True`` when the pair must be regenerated.
    """
    if not cert_path.is_file():
        return True
    try:
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
    except (ValueError, OSError) as exc:
        # Corrupt / unreadable existing cert: regenerate rather than fail (a
        # self-signed loopback cert is disposable). Never `except: pass`.
        logger.warning(
            "existing TLS cert at %s could not be parsed (%s); regenerating",
            cert_path,
            exc,
        )
        return True
    # `not_valid_after_utc` is the timezone-aware accessor (NOT the deprecated
    # naive `not_valid_after`); compare against an aware "now" so the check is
    # tz-correct regardless of the process timezone.
    now = datetime.datetime.now(tz=datetime.UTC)
    if cert.not_valid_after_utc <= now + RENEWAL_SKEW:
        logger.info(
            "TLS cert at %s expires %s (within renewal skew %s of now); regenerating",
            cert_path,
            cert.not_valid_after_utc.isoformat(),
            RENEWAL_SKEW,
        )
        return True
    if not key_path.is_file():
        logger.warning(
            "TLS cert at %s is valid but its private key %s is missing; "
            "regenerating the pair (uvicorn would otherwise fail at bind)",
            cert_path,
            key_path,
        )
        return True
    try:
        # password=None: auto-gen keys are written unencrypted (see the
        # module docstring), so an encrypted file here is not ours and is
        # replaced like any other unusable key.
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    except (ValueError, TypeError, OSError) as exc:
        logger.warning(
            "TLS private key at %s could not be loaded (%s); regenerating the pair",
            key_path,
            exc,
        )
        return True
    if _public_key_der(key) != _public_key_der(cert):
        logger.warning(
            "TLS private key at %s does not match the certificate at %s; "
            "regenerating the pair (a mismatched pair fails the TLS handshake)",
            key_path,
            cert_path,
        )
        return True
    return False


def _generate_self_signed(tls_dir: Path, cert_path: Path, key_path: Path) -> None:
    """Mint a self-signed RSA cert (CN/SAN localhost + 127.0.0.1) and write it.

    The cert PEM is written world-readable-within-dir; the key PEM is written
    ``0o600`` (unencrypted - auto-gen keys carry no passphrase). The ``tls/``
    dir is created ``0o700``.
    """
    try:
        tls_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(tls_dir, _TLS_DIR_MODE)
    except OSError as exc:
        raise TlsCertError(f"could not create TLS directory {tls_dir}: {exc}") from exc

    try:
        key = rsa.generate_private_key(
            public_exponent=_RSA_PUBLIC_EXPONENT,
            key_size=_RSA_KEY_SIZE,
        )
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        san = x509.SubjectAlternativeName(
            [
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            ]
        )
        now = datetime.datetime.now(tz=datetime.UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)  # self-signed: issuer == subject
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + CERT_VALIDITY)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(san, critical=False)
            .sign(key, hashes.SHA256())
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),  # auto-gen keys are unencrypted (see module docstring)
        )
    except (ValueError, TypeError) as exc:
        raise TlsCertError(f"self-signed cert generation failed: {exc}") from exc

    try:
        # Key first, owner-only from the moment of creation (O_CREAT mode +
        # O_TRUNC so a re-gen overwrites a stale key without widening perms).
        key_fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _KEY_FILE_MODE)
        try:
            os.write(key_fd, key_pem)
        finally:
            os.close(key_fd)
        # Re-assert the mode in case the file pre-existed with looser perms
        # (O_CREAT does not chmod an existing file).
        os.chmod(key_path, _KEY_FILE_MODE)
        cert_path.write_bytes(cert_pem)
    except OSError as exc:
        raise TlsCertError(f"could not write TLS cert/key under {tls_dir}: {exc}") from exc

    logger.info(
        "generated self-signed TLS cert at %s (CN=localhost, SAN=localhost/127.0.0.1, "
        "validity=%s); key written 0o600",
        cert_path,
        CERT_VALIDITY,
    )
