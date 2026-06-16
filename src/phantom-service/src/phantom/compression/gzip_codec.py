"""Gzip codec for storage compression."""

from __future__ import annotations

import gzip
from typing import Final, Literal


class GzipCodec:
    """Gzip codec wrapping the stdlib ``gzip`` module."""

    algorithm_name: Final[Literal["gzip"]] = "gzip"

    def __init__(self, level: int) -> None:
        """Construct a codec.

        Args:
            level: gzip compression level (1..9).
        """
        self._level = level

    def encode(self, raw: bytes) -> bytes:
        """Compress ``raw`` at the configured level."""
        return gzip.compress(raw, compresslevel=self._level)

    def decode(self, encoded: bytes) -> bytes:
        """Decompress storage bytes back to the original."""
        return gzip.decompress(encoded)
