# 020. Container image as the deployment artifact

Status: Accepted
Date: 2026-05-27

## Context

Pre-Phase-6 state:

- `src/phantom-deploy/` was scaffolded as a Python package with
  `pyproject.toml` and `src/phantom_deploy/__init__.py`, declared
  as "AWS CDK Python for cloud-side deployment." It was an empty
  stub — no CDK app, no stack definitions.
- `src/phantom-service/docker/` carried **three** Dockerfiles —
  `Dockerfile` (Wolfi pruned), `Dockerfile.alpine`,
  `Dockerfile.wolfi-unpruned` — each building the same service
  with cosmetic differences (base image, debug-symbol stripping).
  ADR-016 codified this as "the phantom-service portion."
- Downstream consumers consume Phantom by **pulling the published
  container image**, never by rebuilding from source. The AWS CDK
  direction in `phantom-deploy/` was speculative; nothing referenced it.

Two simplifications were therefore on the table:

1. Drop the unused CDK package; ship `phantom-deploy/` as the
   container build config + operator README.
2. Collapse the three Dockerfiles into one (Wolfi multi-arch);
   delete the others (no-parallel-schema rule, plan § 0.3).

## Decision

`phantom-deploy/` is **the container image**, not a Python package.
Concretely:

- `src/phantom-deploy/Dockerfile` — single Wolfi-based multi-arch
  Dockerfile (`linux/amd64` + `linux/arm64`). `uv` install;
  `python -m phantom` entrypoint.
- `src/phantom-deploy/docker-compose.yml` — reference compose for
  local dev / single-machine deployments.
- `src/phantom-deploy/README.md` — operator README covering build,
  run, env-var / mounted-YAML configuration.

Deleted:

- `src/phantom-deploy/pyproject.toml`
- `src/phantom-deploy/src/phantom_deploy/__init__.py` + parent
  directories
- `src/phantom-deploy/tests/`
- `aws-cdk-lib` / `constructs` dependencies
- `src/phantom-service/docker/Dockerfile`,
  `src/phantom-service/docker/Dockerfile.alpine`,
  `src/phantom-service/docker/Dockerfile.wolfi-unpruned` — superseded by
  the single `src/phantom-deploy/Dockerfile`.
- The whole `src/phantom-service/docker/` directory.

Preserved:

- The emulator Dockerfile (2026-07-15 amendment: now at
  `src/phantom-emulator/Dockerfile`, the package root; the nested
  in-package copy this ADR originally preserved was stale and was
  removed when the docker-marked e2e lane landed). The emulator is a
  separate package with its own lifecycle; its image is e2e/CI
  infrastructure, built locally and never published.

### Registry

GHCR (`ghcr.io/<org>/phantom-service`). The release-tag CI workflow
(Phase 7) authenticates via the workflow's `GITHUB_TOKEN`; no PAT,
no AWS-side credential.

### Base image

Wolfi (Chainguard) — minimal distroless-equivalent with `apk`
package management, glibc-compatible, multi-arch. The choice was
research-driven; the
old `Dockerfile.alpine` lost on libstdc++-vs-musl complications and
the unpruned Wolfi variant was overweight without offering meaningful
debug-experience advantage.

### Out of scope

- **AWS-specific deployment** (CDK app, CloudFormation stacks,
  ECS / EKS / Lambda integrations). Phantom runs in any
  container-orchestrator that accepts an OCI image; AWS-specific
  glue is downstream-of-Phantom.
- **Helm charts / Kubernetes manifests.** Same logic — downstream.
- **Per-customer images.** The published image is generic; the
  YAML config is the per-deployment override.

## Consequences

### Supersession of ADR-016

ADR-020 **explicitly supersedes the phantom-service portion of
ADR-016**. The old "phantom self-builds two multi-arch container
images" decision stands for the emulator
(`src/phantom-emulator/docker/Dockerfile`) but the phantom-service
portion moves:

- From: `src/phantom-service/docker/Dockerfile{,.alpine,.wolfi-unpruned}` →
  Docker Hub `<docker-org>/phantom-service`.
- To: `src/phantom-deploy/Dockerfile` → GHCR
  `ghcr.io/<org>/phantom-service`.

The phantom-service `latest` tag stops floating to the Docker Hub
location; consumers update their pull source to GHCR per the
operator playbook's "Migration from Docker Hub to GHCR" section.

### phantom-deploy ceases to be a Python package

`uv sync` no longer installs it. The workspace `pyproject.toml`'s
member list drops `phantom-deploy`. The CI workflow's per-package
build matrix no longer includes a `phantom-deploy` wheel build.

### One Dockerfile

No more "which Dockerfile is current" confusion. The single
`src/phantom-deploy/Dockerfile` is the build; differences between
debug vs. release variants live in `docker buildx` target arguments
(if needed), not in parallel files.

## Cross-references

- ADR-016 — the superseded phantom-service container model.
- `src/phantom-deploy/Dockerfile`,
  `src/phantom-deploy/docker-compose.yml`,
  `src/phantom-deploy/README.md`.
- `docs/operator-playbook.md` — deployment topology + migration
  notes.
