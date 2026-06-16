"""Storage-side body compression (req §5d).

Phantom adds a storage-only compression layer (``storage_encoding``
column on each row) independent of the wire ``Content-Encoding``. The
wire encoding is preserved end-to-end; the storage layer is invisible
to upstream.

Always-encode policy: one configured codec per deployment, applied to
every body_ref regardless of size or inbound ``Content-Encoding``. The
transparent-proxy invariant is enforced by the sender's decode path
plus body_hash verification, not by storage-side pass-through.
"""

from __future__ import annotations

from typing import Literal

from phantom.compression.gzip_codec import GzipCodec
from phantom.compression.interface import BodyCodec
from phantom.compression.passthrough_codec import PassthroughCodec
from phantom.compression.zstd_codec import ZstdCodec
from phantom.config.settings import CompressionCfg

CompressionAlgorithm = Literal["original", "zstd", "gzip"]


def select_codec(cfg: CompressionCfg) -> BodyCodec:
    """Return the codec for this deployment's storage compression.

    Always returns the configured codec. There is no size-based
    short-circuit and no content-encoding pass-through; the
    transparent-proxy invariant is enforced by the sender's decode
    path plus body_hash verification, not by storage-side
    pass-through.

    Args:
        cfg: The deployment's :class:`CompressionCfg`. ``cfg.level`` is
            the single shared codec compression level; ``cfg.algorithm``
            picks which codec to instantiate.

    Returns:
        A :class:`BodyCodec` instance to use for this body.

    Raises:
        ValueError: When ``cfg.algorithm`` is not one of the known
            ``CompressionAlgorithm`` literals.
    """
    if cfg.algorithm == "zstd":
        return ZstdCodec(level=cfg.level)
    if cfg.algorithm == "gzip":
        return GzipCodec(level=cfg.level)
    if cfg.algorithm == "original":
        return PassthroughCodec()
    raise ValueError(f"Unknown compression algorithm: {cfg.algorithm}")


def build_codec_for_algorithm(
    algorithm: CompressionAlgorithm,
    *,
    level: int = 3,
) -> BodyCodec:
    """Construct a codec given a row's ``storage_encoding`` value.

    Args:
        algorithm: The row's stored algorithm marker.
        level: Codec level to use for re-encoding (unused for decode).

    Returns:
        A codec capable of decoding bytes stored under that algorithm.
    """
    if algorithm == "zstd":
        return ZstdCodec(level=level)
    if algorithm == "gzip":
        return GzipCodec(level=level)
    return PassthroughCodec()


__all__ = [
    "BodyCodec",
    "CompressionAlgorithm",
    "GzipCodec",
    "PassthroughCodec",
    "ZstdCodec",
    "build_codec_for_algorithm",
    "select_codec",
]
