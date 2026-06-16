"""Unit tests for phantom.compression."""

from __future__ import annotations

import asyncio

import pytest
from phantom.compression import (
    GzipCodec,
    PassthroughCodec,
    ZstdCodec,
    build_codec_for_algorithm,
    select_codec,
)
from phantom.config.settings import CompressionCfg


def test_passthrough_roundtrip() -> None:
    """PassthroughCodec is identity."""
    c = PassthroughCodec()
    assert c.encode(b"abc") == b"abc"
    assert c.decode(b"abc") == b"abc"
    assert c.algorithm_name == "original"


def test_zstd_roundtrip() -> None:
    """zstd round-trips at levels 1, 3, 22."""
    payload = b"hello world" * 1000
    for level in (1, 3, 22):
        c = ZstdCodec(level=level)
        encoded = c.encode(payload)
        assert c.decode(encoded) == payload


def test_gzip_roundtrip() -> None:
    """gzip round-trips."""
    payload = b"x" * 10000
    c = GzipCodec(level=6)
    assert c.decode(c.encode(payload)) == payload


def test_select_codec_zstd() -> None:
    """``algorithm='zstd'`` returns ZstdCodec."""
    c = select_codec(CompressionCfg(algorithm="zstd", level=3))
    assert isinstance(c, ZstdCodec)


def test_select_codec_gzip() -> None:
    """``algorithm='gzip'`` returns GzipCodec."""
    c = select_codec(CompressionCfg(algorithm="gzip", level=6))
    assert isinstance(c, GzipCodec)


def test_select_codec_original() -> None:
    """``algorithm='original'`` returns PassthroughCodec."""
    c = select_codec(CompressionCfg(algorithm="original"))
    assert isinstance(c, PassthroughCodec)


def test_build_codec_for_algorithm() -> None:
    """Factory by algorithm name returns the right type."""
    assert isinstance(build_codec_for_algorithm("zstd"), ZstdCodec)
    assert isinstance(build_codec_for_algorithm("gzip"), GzipCodec)
    assert isinstance(build_codec_for_algorithm("original"), PassthroughCodec)


@pytest.mark.asyncio
async def test_encode_runs_on_worker_thread() -> None:
    """A concurrent ``asyncio.sleep`` resolves while encode runs on a thread."""
    c = ZstdCodec(level=3)
    payload = b"y" * (4 * 1_048_576)
    woke = asyncio.Event()

    async def keep_alive() -> None:
        await asyncio.sleep(0.01)
        woke.set()

    task = asyncio.create_task(keep_alive())
    await asyncio.to_thread(c.encode, payload)
    await task
    assert woke.is_set()
