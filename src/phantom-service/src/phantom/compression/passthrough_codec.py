"""Passthrough codec - no-op encode/decode."""

from __future__ import annotations

from typing import Final, Literal


class PassthroughCodec:
    """Identity codec: ``encode`` and ``decode`` are no-ops."""

    algorithm_name: Final[Literal["original"]] = "original"

    def encode(self, raw: bytes) -> bytes:
        """Return ``raw`` unchanged."""
        return raw

    def decode(self, encoded: bytes) -> bytes:
        """Return ``encoded`` unchanged."""
        return encoded
