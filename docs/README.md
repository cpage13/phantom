# Phantom design documentation

This directory is the durable design knowledge for Phantom. Code under `src/` is the source of truth for *what works*; this directory is the source of truth for *why it's shaped that way* and *how to operate it*.

## I just want to do one specific thing

| If you want to... | Read this |
|---|---|
| Deploy Phantom in production | [`operator-playbook.md`](operator-playbook.md) |
| Understand the runtime topology in one read | [`architecture-intent.md`](architecture-intent.md) |
| Read the full engineering breakdown of how robust it is | [`engineering/`](engineering/) |
| Know why a decision was made the way it was | [`adr/`](adr/) (pick the ADR by topic) |
| Find a specific term used across code + docs | [`../CONTEXT.md`](../CONTEXT.md) |

## What lives where

### `architecture-intent.md`: read once to orient

Single-file map of the system: what Phantom is, its runtime topology, the reliability invariants (two actively asserted, five more counter/gauge monitored), the core admission → buffer → upload flow, deployment-target constraints, failure modes, and the CI shape. Aim for one read on first contact; come back for specific sections.

### `engineering/`: read for the full picture of how robust it is

Three reader-facing deep dives, written to be read top to bottom. `architecture.md` covers what Phantom is, the request lifecycle, the storage design, and the database retry mechanisms. `test-suite.md` tours the end-to-end suite (functional, performance, aggressor, and reliability tests). `reliability-and-security.md` covers error handling, the fallback procedure for every failure mode, security, and the robustness guarantees. They explain and aggregate what the code and the ADRs establish.

### `operator-playbook.md`: read when deploying or operating

Deployment topology, mode-selection guide (`hybrid` / `all_ram` / `all_disk`), configuration walk-through against `config/phantom.yaml.example`, observability and alerting on the `/v1/admin/observability/*` endpoints, failure-mode diagnosis (DB quarantine, RAM pressure, 413 body_too_large, 401 auth_expired), and a YAML migration table for pre-release deployments.

### `adr/`: read the ADR for the area you're touching

One ADR per decision. Each is short and scoped. The decision is authoritative for whatever it covers; anything older that contradicts an ADR is superseded.

Don't read all ADRs on session start. Scan the filenames in `adr/`. When you're about to change code in an area an ADR covers, read that ADR first.

A few you'll hit often:

- `001-opaque-core-with-pluggable-refresh.md`: the auth model.
- `004-admin-api-loopback-no-auth.md`: why admin endpoints have no auth.
- `010-request-chain-envelope-schema.md`: the wire protocol Pydantic schema.
- `014-dual-body-hash.md`: body-equality enforcement + the runtime missing-body contract.
- `017-error-code-matrix.md`: every HTTP status + reason code Phantom emits.
- `019-atomic-transaction-idempotency.md`: admission's atomic insert.
- `020-container-image-as-deployment-artifact.md`: container is the deployment.

### `design-history/`: archived only

Pre-implementation design documents from earlier cycles. Historical reference, kept for traceability. Don't act on these; the ADRs and `architecture-intent.md` supersede them.

## How authority works

- An ADR is authoritative for the decision it records. It supersedes anything older it contradicts. Don't reopen a settled decision without surfacing the ADR first.
- `architecture-intent.md` integrates the current ADRs into a single map. If it conflicts with an ADR, the ADR wins (and the intent doc has a stale section to fix).
- `operator-playbook.md` is the day-to-day operational manual. If a procedure here conflicts with code behavior, the code wins and the playbook has drift to fix.
- Anything in `design-history/` is non-authoritative.

## Reading order for new readers

1. [`../CONTEXT.md`](../CONTEXT.md): concept glossary. Short. Read first so the terms you'll see in code and ADRs are anchored.
2. [`architecture-intent.md`](architecture-intent.md): one-read orientation to the system.
3. [`operator-playbook.md`](operator-playbook.md): if you're operating it.
4. Specific ADRs by topic, as you touch the code they govern.
