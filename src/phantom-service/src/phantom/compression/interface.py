"""BodyCodec Protocol — Phantom-side storage compression."""

from __future__ import annotations

from typing import Literal, Protocol


class BodyCodec(Protocol):
    """Storage-side body codec.

    ``encode`` / ``decode`` are **synchronous** by design — the
    underlying zstd / gzip extensions release the GIL inside their
    C extensions, so every caller wraps the call with
    ``await asyncio.to_thread(codec.encode, body)`` for parallelism.
    """

    @property
    def algorithm_name(self) -> Literal["original", "zstd", "gzip"]:
        """The algorithm marker recorded on the row's ``storage_encoding`` column.

        Read-only — each concrete codec hard-codes its value as a
        ``Final`` class attribute.
        """
        ...

    def encode(self, raw: bytes) -> bytes:
        """Compress ``raw`` and return the storage bytes."""
        ...

    def decode(self, encoded: bytes) -> bytes:
        """Decompress storage bytes back to the original."""
        ...
