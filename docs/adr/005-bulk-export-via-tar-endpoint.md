# 005. Bulk emergency export via streaming tar endpoint

Phantom exposes a single admin endpoint `GET /v1/admin/export.tar` that streams a tar archive containing every buffered file's body plus a top-level `manifest.json` capturing per-row metadata: `chain_id`, `instance_id`, `group_id`, `state`, `endpoint`, `received_at`, `sent_at` (null until confirmed delivery), `body_size_bytes`, `storage_encoding`. This covers the "device-in-the-field cannot reach upstream, recover the buffered files with curl" use case without requiring any client-side tooling: a single `curl http://localhost:8080/v1/admin/export.tar > export.tar` is the whole operation (admin rides the single listener; loopback by default). File bodies are preserved in their stored encoding; the endpoint is strictly read-only, and deletion of buffered files remains a separate explicit operation under `DELETE /v1/admin/chains` per ADR-004. A CLI wrapping this endpoint plus other admin verbs is a possible future addition (it would live in `src/phantom-service/` and ship in the image).

Status: Accepted
Date: 2026-05-12
