"""Per-codec round-trip property tests.

For each codec (passthrough, gzip, zstd) and each input class (empty,
1 byte, 100 KiB, 16 MiB, random binary, repeated pattern), assert
``codec.decode(codec.encode(x)) == x`` and that the SHA-256 of the
round-tripped bytes matches the original.
"""

from __future__ import annotations

import hashlib
import os

import pytest
from phantom.compression import GzipCodec, PassthroughCodec, ZstdCodec
from phantom.compression.interface import BodyCodec

# Test-input size grid. The 16 MiB case is the upper bound the test
# exercises; we keep it modest enough to keep the test suite fast.
_SIZES_BYTES: list[int] = [0, 1, 100, 100_000, 16 * 1024 * 1024]


def _codecs() -> list[BodyCodec]:
    return [
        PassthroughCodec(),
        GzipCodec(level=6),
        ZstdCodec(level=3),
    ]


def _shape_inputs(size: int) -> list[bytes]:
    """Three payload classes per size: zeros, repeated pattern, random."""
    if size == 0:
        return [b""]
    return [
        b"\x00" * size,
        (b"abc" * ((size // 3) + 1))[:size],
        os.urandom(size),
    ]


@pytest.mark.parametrize("size", _SIZES_BYTES)
def test_codec_round_trip_for_each_codec(size: int) -> None:
    """``decode(encode(x)) == x`` and hash identity for every codec / size pair."""
    for codec in _codecs():
        for payload in _shape_inputs(size):
            encoded = codec.encode(payload)
            decoded = codec.decode(encoded)
            assert decoded == payload, (
                f"codec={codec.algorithm_name!r} size={size} did not round-trip"
            )
            # SHA-256 identity sanity-check — the test belt-and-braces
            # the body_hash contract the sender enforces at runtime.
            assert hashlib.sha256(decoded).hexdigest() == hashlib.sha256(payload).hexdigest()
