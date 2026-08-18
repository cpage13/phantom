"""In-RAM body store.

Per-upload bodies live as ``dict[UUID, dict[name, bytes]]`` keyed by
``chain_id``. A lock guards the dict against rare admin-list-vs-mutate
races.

Plan § 2.3.8 dropped the ``tier`` property - the
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
        # Running sum of every stored byte. Maintained at the three points
        # that mutate ``_bodies``, all of which already hold ``_lock`` (U11).
        self._total_bytes = 0
        self._lock = asyncio.Lock()
        self._started = False

    async def start(self) -> None:
        """No-op; RAM store needs no setup."""
        self._started = True

    async def stop(self) -> None:
        """Drop every body."""
        async with self._lock:
            self._bodies.clear()
            self._total_bytes = 0
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
        stored = sum(len(b) for b in body_refs.values())
        async with self._lock:
            # REPLACE semantics, so the delta is measured against the whole
            # entry this call displaces, not against the bytes written (U11).
            displaced = sum(len(b) for b in self._bodies.get(chain_id, {}).values())
            self._bodies[chain_id] = dict(body_refs)
            self._total_bytes += stored - displaced
        return stored

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
            dropped = self._bodies.pop(chain_id, {})
            self._total_bytes -= sum(len(b) for b in dropped.values())

    async def total_bytes(self) -> int:
        """Saturation accounting - sum of stored body bytes.

        Returns a RUNNING COUNTER rather than re-summing every ref of every
        entry (U11). The sum was O(all refs) under ``_lock``, which every
        other method also takes, and the RAM-pressure watcher samples it once
        per tick AND again after each of up to 64 enqueued candidates, so the
        cost peaked exactly when the store was largest.

        The counter is maintained at the three points that mutate
        ``_bodies``: :meth:`stop` zeroes it, :meth:`put` applies the
        replaced entry's delta, and :meth:`delete` subtracts what it popped.

        The lock is KEPT deliberately. It costs nothing once the body is
        O(1), and dropping it would change this store's concurrency contract
        in a change whose whole point is that no observable moves.
        """
        async with self._lock:
            return self._total_bytes

    async def list_chain_ids(self) -> list[UUID]:
        """Return the set of chain_ids with stored bodies."""
        async with self._lock:
            return list(self._bodies.keys())

    async def list_orphans(self, known_chain_ids: set[UUID]) -> list[UUID]:
        """Return RAM-tier orphans relative to ``known_chain_ids``.

        Always returns ``[]``. RAM bodies have no orphans by
        construction - the dict is mutated synchronously with row
        deletes through the same code paths (admission clears the
        chain_id namespace then writes ``put``; reaper / cancel /
        replay call ``delete``), so a chain_id present in RAM is
        always present in ``uploads``.
        The method exists so the body-orphan janitor
        (plan § 2.3.14) can call it through the :class:`BodyStore`
        Protocol without branching on the concrete binding.
        """
        return []
