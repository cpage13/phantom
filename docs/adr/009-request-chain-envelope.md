# 009. Request-chain envelope for multi-step buffered uploads

Phantom accepts a **request chain** envelope: a sequence of HTTP steps where each step can capture specific fields from its response into named variables (via JSONPath), and subsequent steps reference those variables in their URL, headers, or body via `{{step.var}}` template substitution; binary body data rides as named `body_refs` alongside the JSON envelope. The driving case is two steps (the upstream metadata POST captures `upload_url` and `file_information`; then S3 PUT to `{{create_file.upload_url}}` with the body bytes), but the mechanism handles N steps if ever needed; each step can declare an `idempotency_header` (Phantom sends a per-chain idempotency key the upstream uses to dedup retries — required for safe re-execution of step 1 when later steps need a fresh response) and a `capture_ttl_seconds` per captured variable (Phantom re-executes the producing step if a captured value expires before later steps complete, e.g. presigned URLs at a 7-day max TTL). Captured response values are stored on the chain row and remain queryable via admin API for the duration of the success-metadata retention window (default `succeeded_metadata_seconds: 180` — 3 minutes); the file body itself is released immediately on success (`succeeded_body_seconds: 0` default). Deployments that need longer local-UUID → upstream-id correlation can raise `succeeded_metadata_seconds` in their YAML.

## Amendment — 2026-07-12: reexecution safety has two independent contracts

The paragraph above records the original motivating assumption. ADR-011 now
governs capture reexecution. An `idempotency_header` remains optional at
runtime: when the producing step omits it, Phantom still reexecutes after an
enabled capture-observation expiry, and the upstream may create a duplicate.
When declared, the header is required for **identity-safe** reexecution, but
identity deduplication alone does not renew a URL/token returned in an exact
cached response. Enabling `capture_reexecution` therefore requires verifying
both the upstream's identity/idempotency behavior and compatible capability
renewal or lifetime. `ChainCapture.ttl_seconds` drives Phantom's observation
clock; it is not proof of actual upstream capability expiry.

## Amendment (2026-08-17): substitution is conditional on `templated`

The paragraph above describes substitution as a property of the mechanism,
which it no longer is unconditionally. The envelope carries a `templated`
marker (ADR-010's schema; finding N3 of the review-08-12 cycle). It defaults
to `true`, so every chain the paragraph describes behaves exactly as written.
A chain marked `templated: false` declares that its brace spans are content:
no substitution runs, the capture-TTL gate does not run, and the parser's
static placeholder pass is skipped at admission. Phantom's raw-intake
catch-all sets it, because a stock object key may legally contain a `{{...}}`
span and interpreting one destroyed a valid upload.

Status: Accepted
Date: 2026-05-12
