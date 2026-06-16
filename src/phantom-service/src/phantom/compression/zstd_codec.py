"""Zstandard codec for storage compression."""

from __future__ import annotations

from typing import Final, Literal

import zstandard


class ZstdCodec:
    """Zstandard codec.

    ``encode`` releases the GIL inside the C extension; callers should
    wrap with ``await asyncio.to_thread(codec.encode, body)`` to keep
    the event loop responsive.
    """

    algorithm_name: Final[Literal["zstd"]] = "zstd"

    def __init__(self, level: int) -> None:
        """Construct a codec.

        Args:
            level: zstd compression level (1..22). 3 is the default
                trade-off for speed vs. ratio.
        """
        self._level = level
        self._compressor = zstandard.ZstdCompressor(level=level)
        self._decompressor = zstandard.ZstdDecompressor()

    def encode(self, raw: bytes) -> bytes:
        """Compress ``raw`` using the configured level."""
        return self._compressor.compress(raw)

    def decode(self, encoded: bytes) -> bytes:
        """Decompress storage bytes back to the original."""
        return self._decompressor.decompress(encoded)
