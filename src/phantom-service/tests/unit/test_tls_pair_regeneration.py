"""S3-8. The auto-gen TLS path heals the PAIR, not just the certificate.

``_needs_regeneration`` inspected only the certificate. A valid cert beside
a missing, unreadable or MISMATCHED private key was therefore returned
unchanged, and the process died later inside uvicorn's ``load_cert_chain``
with a ``FileNotFoundError`` or an ``ssl.SSLError`` - after ``create_app``
had already logged a healthy factory. Because a minted cert stays valid for
``CERT_VALIDITY`` (825 days), every restart took that identical branch: a
self-healing path that could not heal the half that was actually broken.

Each test asserts the property uvicorn actually needs at bind time - the
returned pair loads into a real :class:`ssl.SSLContext` - which is the
exact operation that failed before the fix, on the same input.
"""

from __future__ import annotations

import ssl
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from phantom.config.settings import TlsCfg
from phantom.runtime.tls_cert import resolve_tls_paths

# A throwaway key size for the mismatch fixture. Small enough to be fast,
# large enough for the cryptography backend to accept.
_FOREIGN_KEY_SIZE = 2048
_RSA_PUBLIC_EXPONENT = 65537


def _write_foreign_key(key_path: Path) -> None:
    """Overwrite ``key_path`` with a VALID key from a different pair."""
    key = rsa.generate_private_key(public_exponent=_RSA_PUBLIC_EXPONENT, key_size=_FOREIGN_KEY_SIZE)
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )


def _assert_pair_binds(cert_path: str, key_path: str) -> None:
    """Assert the pair loads exactly as uvicorn loads it at bind time."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)


def test_a_valid_cert_with_a_missing_key_regenerates_the_pair(tmp_path: Path) -> None:
    """S3-8. Deleting the key must re-mint both halves.

    Objective: the cert is untouched and valid for years, so the
    cert-only check reuses it and hands back a key path that does not
    exist. Expected outcome: the resolve regenerates, and the returned
    pair loads into an SSLContext. Before the fix
    ``load_cert_chain`` raised ``FileNotFoundError`` here.
    """
    cfg = TlsCfg(enabled=True)
    cert_path, key_path = resolve_tls_paths(cfg, str(tmp_path))
    Path(key_path).unlink()

    cert_path_again, key_path_again = resolve_tls_paths(cfg, str(tmp_path))

    assert Path(key_path_again).is_file(), "the missing key must be re-minted"
    _assert_pair_binds(cert_path_again, key_path_again)
    assert (cert_path_again, key_path_again) == (cert_path, key_path), (
        "regeneration must reuse the STABLE auto-gen paths"
    )


def test_a_valid_cert_with_a_mismatched_key_regenerates_the_pair(tmp_path: Path) -> None:
    """S3-8. A key from a different pair must re-mint both halves.

    Objective: the nastier half of the finding. Both files exist and both
    parse, so nothing looks wrong to a cert-only check, but the private key
    does not belong to the certificate. Expected outcome: the resolve
    regenerates and the returned pair loads. Before the fix
    ``load_cert_chain`` raised ``ssl.SSLError`` ("key values mismatch").
    """
    cfg = TlsCfg(enabled=True)
    _cert_path, key_path = resolve_tls_paths(cfg, str(tmp_path))
    _write_foreign_key(Path(key_path))

    cert_path_again, key_path_again = resolve_tls_paths(cfg, str(tmp_path))

    _assert_pair_binds(cert_path_again, key_path_again)


def test_a_valid_cert_with_an_unparseable_key_regenerates_the_pair(tmp_path: Path) -> None:
    """S3-8. A truncated or corrupt key must re-mint both halves.

    Objective: the same posture the module already takes for a corrupt
    certificate - a self-signed loopback pair is disposable, so an
    unusable half is replaced rather than raised over. Expected outcome:
    the resolve regenerates and the returned pair loads.
    """
    cfg = TlsCfg(enabled=True)
    _cert_path, key_path = resolve_tls_paths(cfg, str(tmp_path))
    Path(key_path).write_bytes(b"-----BEGIN RSA PRIVATE KEY-----\nnot a real key\n")

    cert_path_again, key_path_again = resolve_tls_paths(cfg, str(tmp_path))

    _assert_pair_binds(cert_path_again, key_path_again)
