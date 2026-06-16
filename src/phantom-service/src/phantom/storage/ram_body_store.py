"""In-RAM body store.

Per-upload bodies live as ``dict[UUID, dict[name, bytes]]`` keyed by
``chain_id``. A lock guards the dict against rare admin-list-vs-mutate
races.

Plan § 2.3.8 dropped the ``tier`` property — the
Protocol no longer carries it. Added :meth:`list_orphans` returning
``[]`` (RAM bodies have no orphans by construction; the dict is
purged on chain drop).
"""

from __future__ import annotations

import asyncio
from uuid import UUID


class RamBodyStore:
    """In-RAM body store.

    No ``tier`` property; ``list_orphans`` added so the
    body-orphan janitor can call it through the Protocol
    without branching on the binding type.
    """

    def __init__(self) -> None:
        """Initialize an empty store."""
        self._bodies: dict[UUID, dict[str, bytes]] = {}
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        """No-op; RAM store needs no setup."""
        self._started = True

    async def stop(self) -> None:
        """Drop every body."""
        async with self._lock:
            self._bodies.clear()
        self._started = False

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        """Store every named body_ref for ``chain_id``.

        REPLACE semantics (R11-a): the whole chain entry is replaced
        by ``body_refs`` - any previously stored ref name not in the
        mapping is dropped. This is the strict end of the
        :class:`BodyStore` put contract; :class:`FileBodyStore.put`
        sits at the additive end (see the Protocol docstring).

        Args:
            chain_id: Upload identifier (the chain's id).
            body_refs: Mapping of body_ref name → bytes.

        Returns:
            Total bytes stored for this upload.
        """
        async with self._lock:
            self._bodies[chain_id] = dict(body_refs)
        return sum(len(b) for b in body_refs.values())

    async def get(self, chain_id: UUID, name: str) -> bytes:
        """Read one named body_ref."""
        async with self._lock:
            try:
                return self._bodies[chain_id][name]
            except KeyError as exc:
                raise KeyError(f"No body_ref {name!r} for chain_id={chain_id}") from exc

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Read every body_ref for ``chain_id``."""
        async with self._lock:
            try:
                return dict(self._bodies[chain_id])
            except KeyError as exc:
                raise KeyError(f"No body refs for chain_id={chain_id}") from exc

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        """Return whether a body_ref named ``name`` exists for ``chain_id``."""
        async with self._lock:
            return chain_id in self._bodies and name in self._bodies[chain_id]

    async def delete(self, chain_id: UUID) -> None:
        """Drop all body_refs for ``chain_id``. Idempotent."""
        async with self._lock:
            self._bodies.pop(chain_id, None)

    async def total_bytes(self) -> int:
        """Saturation accounting — sum of stored body bytes."""
        async with self._lock:
            return sum(len(b) for refs in self._bodies.values() for b in refs.values())

    async def list_chain_ids(self) -> list[UUID]:
        """Return the set of chain_ids with stored bodies."""
        async with self._lock:
            return list(self._bodies.keys())

    async def list_orphans(self, known_chain_ids: set[UUID]) -> list[UUID]:
        """Return RAM-tier orphans relative to ``known_chain_ids``.

        Always returns ``[]``. RAM bodies have no orphans by
        construction — the dict is mutated synchronously with row
        deletes through the same code paths (admission clears the
        chain_id namespace then writes ``put``; reaper / cancel /
        replay call ``delete``), so a chain_id present in RAM is
        always present in ``uploads``.
        The method exists so the body-orphan janitor
        (plan § 2.3.14) can call it through the :class:`BodyStore`
        Protocol without branching on the concrete binding.
        """
        return []
