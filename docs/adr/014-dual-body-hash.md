# 014. Dual body hash: storage_hash and body_hash per body_ref

Each body_ref on every `UploadRow` carries a `BodyHashes(body_hash, storage_hash)` pair. `body_hash` is the SHA-256 of the raw bytes the agent submitted; `storage_hash` is the SHA-256 of the bytes Phantom actually stored (after the configured codec encodes them). Both are computed at ingress in the admission orchestrator (`routes/admission.py`) before the row is inserted, off the event loop via `asyncio.to_thread`.

The sender verifies both before forwarding to the upstream. On each body load:

1. Read the stored bytes; compute SHA-256; compare to `row.body_hashes[name].storage_hash`. Mismatch raises `StorageCorruptionError` — the body file was tampered with, partially written, or the disk lied. Row transitions to `corrupted` (terminal) with `last_error="storage_corruption:..."`.
2. Decode via the row's codec; compute SHA-256 of the decoded bytes; compare to `row.body_hashes[name].body_hash`. Mismatch raises `CodecRoundTripDriftError` — the codec lost data or its round-trip is non-identity. Row transitions to `corrupted` with `last_error="codec_round_trip_drift:..."`. This catches codec bugs that storage-hash verification cannot.

`corrupted` is a terminal state. Sender never retries it. The reaper iterates corrupted rows on the same sweep as the other terminal states (retention defaults: 30 days metadata and body, matching `failed`).

The transparent-proxy invariant — "bytes Phantom forwards to upstream are byte-identical to what the agent sent" — is thereby enforced, not just claimed. A test surface (`tests/e2e/test_transparent_proxy.py`) hashes the agent's body, runs it through Phantom, hashes the body the emulator received, and asserts equality across the three codecs and both submission shapes (JSON inline base64 / multipart body_refs). The phantom-client multipart fix (every part carries a non-empty filename) is load-bearing for this invariant — starlette's MultiPartParser silently UTF-8-decodes filename-less parts, corrupting any body with bytes >= 0x80.

**Note on passthrough.** The always-encode rule (one configured codec per deployment; default zstd) requires every body to round-trip through the codec. `PassthroughCodec` (storage token `"original"`) remains available as an explicit operator choice for deployments where the upstream expects pre-encoded bytes — for example, a client that already gzips its bodies and wants Phantom to forward them unchanged with zero CPU overhead. Operators pin this via `compression.algorithm: original` in YAML. It is NOT auto-selected; the always-encode invariant means every body goes through the chosen codec, whichever one that is. Under `algorithm: original`, `storage_hash` and `body_hash` are equal by construction — passthrough is the identity codec — but the verification still runs and still catches disk corruption.

Status: Accepted
Date: 2026-05-14

## Update — 2026-05-27 (Phase 2 H8 + Phase 1 recovery)

The dual-hash verification described above assumes the body file
exists when the sender attempts to load it. Two additional code
paths handle the file-missing case:

### Runtime missing-body contract (Phase 2 § 3.2.6 — H8 closure)

`HybridBodyStore.load_body_refs` raises `BodyMissingError` when a
body file is referenced by a row but not present in the body store
(neither RAM nor disk). The sender's `_drive_one` cascade catches
this and routes the row to `corrupted` with
`last_error="storage_corruption:bodies_missing"`. No retry. Same
terminal posture as the hash-mismatch path above.

The structural cause is "body file vanished between admission and
the sender's first attempt" — operator file-system intervention,
container volume swap, or a real corruption event. The terminal
state is what the operator wants in every case: the row will not
silently succeed, and the body bytes are not coming back.

### Recovery sweep body-existence walk (Phase 1 § 2.3.15)

`workers/recovery.py` runs at startup, before the sender pool
spawns. For each row with `body_location='file'` that is not in a
terminal state AND not subject to the H4 carve-out
(`body_discarded_at IS NULL`), recovery verifies that every key in
`body_hashes` has a corresponding on-disk file via
`BodyStore.has_body_ref`. Missing files transition the row to
`corrupted` with `last_error="storage_corruption:bodies_missing_at_recovery"`.

The H4 carve-out (Phase 2 § 3.2.4) preserves the deliberate-discard
case: rows in `auth_expired` whose bodies were swept by the reaper
per `auth_expired_body_seconds` retention have `body_discarded_at`
set; recovery does not flag those as corrupted.

`body_location='ram'` rows after restart are a separate case — the
RAM body store is empty after restart by design; recovery
quarantines those rows. They never reach the sender's body-load
path because the recovery quarantine completes first.

### Summary of the four corruption-detection paths

| Path | Where | Trigger | Resulting `last_error` |
|---|---|---|---|
| `storage_hash` mismatch | Sender body load | Disk corruption / tampering / partial write | `storage_corruption:hash_mismatch` |
| `body_hash` mismatch (post-decode) | Sender body load | Codec round-trip drift (codec bug) | `codec_round_trip_drift:hash_mismatch` |
| `BodyMissingError` | Sender body load | File absent at attempt time | `storage_corruption:bodies_missing` |
| Recovery body-existence walk | Startup, pre-sender | File absent at boot for non-terminal `body_location='file'` row (not H4-carved-out) | `storage_corruption:bodies_missing_at_recovery` |

All four transition the row to the `corrupted` terminal state and
emit `storage_corruption` / `codec_round_trip_drift` as ADR-017
row-level error codes (queryable via
`GET /v1/admin/chains/{chain_id}.last_error`). See
`docs/architecture-intent.md` § 7 failure modes for the operator-
facing recovery contract.
