# 002. Token cache keyed by (endpoint, credential identifier)

Phantom's token cache is keyed by the tuple `(endpoint, credential identifier)`, where `endpoint` is the upstream hostname (e.g., `files.upstream.example`) and `credential identifier` is an opaque string supplied by the caller on each request via a Phantom-specific header. Phantom treats the identifier as an opaque hashmap key — it never parses the underlying credential or derives the identifier itself; the upstream client is responsible for producing it. This preserves the opaque-core invariant from ADR-001 (Phantom does no token introspection) while supporting multiple concurrent identities at the same endpoint — e.g., multiple uploaders on a v0 deployment, or a producer service-principal alongside per-upload uploader overrides on v1 — without cross-attribution between buffered uploads.

Status: Accepted
Date: 2026-05-12
