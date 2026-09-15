"""Case-insensitive writes into the outbound header dict.

The outbound header map Phantom builds is a plain ``dict[str, str]``, because
that is what :class:`~phantom.transport.interface.UpstreamRequest` carries and
what botocore's signer is handed. A plain dict is case-SENSITIVE and HTTP
header names are not, and every duplicate-header defect this module exists to
prevent is that one mismatch:

* ASGI lower-cases inbound header names, so a raw-intake step persists
  ``authorization`` / ``content-type``, while Phantom's own writes used the
  canonical spelling. ``headers["Authorization"] = ...`` beside a persisted
  ``authorization`` puts BOTH raw names on the wire (verified against the
  pinned httpx), which discloses the producer's credential upstream on the one
  route whose purpose is to substitute Phantom's own.
* ``"Content-Type" not in headers`` answers False against a persisted
  ``content-type``, so a second Content-Type is stamped and S3 recombines the
  pair into ``image/jpeg,application/octet-stream``.

The rule is therefore a property of the WRITE, not of each comparison: a write
removes every case-insensitive spelling of the name before inserting its own,
so no name can appear twice however the producer spelled it. There is no
case-insensitive mapping TYPE here on purpose - the type would have to cross
the transport interface and botocore's signer, neither of which this package
owns, while the three functions below make the guarantee structural at every
site that writes.

Insertion order of an existing name is not preserved: a replaced header moves
to the end of the dict. HTTP assigns no meaning to header order, and httpx
emits whatever order the mapping iterates.
"""

from __future__ import annotations


def find_header(headers: dict[str, str], name: str) -> str | None:
    """Return the key in ``headers`` that spells ``name``, case-insensitively.

    Args:
        headers: The outbound header map.
        name: The header name to look for, in any casing.

    Returns:
        The existing key as spelled, or ``None`` when the name is absent. The
        KEY rather than the value, because callers that find one need to
        overwrite it in place or report it.
    """
    lowered = name.lower()
    return next((key for key in headers if key.lower() == lowered), None)


def has_header(headers: dict[str, str], name: str) -> bool:
    """Report whether ``headers`` already carries ``name`` in any casing.

    Args:
        headers: The outbound header map.
        name: The header name to test, in any casing.

    Returns:
        ``True`` when some spelling of ``name`` is present.
    """
    return find_header(headers, name) is not None


def set_header(headers: dict[str, str], name: str, value: str) -> None:
    """Set ``name`` to ``value``, dropping every other spelling of it.

    Mutates in place: the executor and botocore's signer both read the same
    mapping object back, so it is never rebound.

    Args:
        headers: The outbound header map, mutated in place.
        name: The header name to write, in the casing to send.
        value: The header value.
    """
    lowered = name.lower()
    for key in [key for key in headers if key.lower() == lowered]:
        del headers[key]
    headers[name] = value


def pop_header(headers: dict[str, str], name: str) -> str | None:
    """Remove every spelling of ``name`` and return the last value removed.

    Args:
        headers: The outbound header map, mutated in place.
        name: The header name to remove, in any casing.

    Returns:
        The removed value, or ``None`` when the name was absent.
    """
    lowered = name.lower()
    removed: str | None = None
    for key in [key for key in headers if key.lower() == lowered]:
        removed = headers.pop(key)
    return removed


__all__ = ["find_header", "has_header", "pop_header", "set_header"]
