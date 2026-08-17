"""Byte-preserving query-string folding.

A leaf module: it imports ``urllib.parse`` and ``collections.abc`` and nothing
from Phantom, so both the raw-intake catch-all (which strips Phantom's own
``phantom`` carrier before forwarding) and the executor (which strips a
superseded presigned credential set on an ``aws_sigv4`` route) can share ONE
implementation of the rule without inverting a dependency edge.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import unquote_plus


def filter_raw_query(raw: str, *, keep: Callable[[str], bool]) -> str:
    """Rebuild a raw query string from the segments whose decoded key ``keep`` accepts.

    THE one byte-preserving query fold. Splits on ``&`` and reassembles the
    surviving segments verbatim, so percent-encoding, ``+`` versus ``%20``,
    parameter order and repeated keys all survive exactly; an S3 presigned
    signature is computed over the canonical query string, so a
    ``parse_qsl``/``urlencode`` round trip would silently invalidate it. The
    key handed to ``keep`` is ``unquote_plus``-decoded, because callers compare
    against names as a parsed view would produce them.

    Args:
        raw: The query text with no leading ``?``.
        keep: Predicate over the DECODED key of each segment.

    Returns:
        The surviving query text, or ``""``.
    """
    if not raw:
        return ""
    return "&".join(
        segment
        for segment in raw.split("&")
        if segment and keep(unquote_plus(segment.split("=", 1)[0]))
    )
