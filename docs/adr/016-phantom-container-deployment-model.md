# 016. Phantom container deployment model

Phantom self-builds and publishes two multi-arch (`linux/arm64` + `linux/amd64`) container images: `phantom-service` (the buffering upload-proxy) and `phantom-emulator` (the upstream-shaped test server). The registry is **public Docker Hub** under the `<docker-org>` organization. Consumers deploy by pulling the published tag — Phantom is **not** rebuilt by downstream consumers (notably a downstream Balena overlay does `image: <docker-org>/phantom-service:<tag>`, not `build: ...`).

Tag scheme:

- **`<version>`** — immutable, reproducible. The current cycle ships `0.1.0`. Versioned tags are what downstreams pin in production; once pushed, a versioned tag is never re-pushed.
- **`latest`** — floating; tracks the most recent stable build. Convenient for development and one-off smokes; **never pin `latest` in production deployments** because it moves under the consumer's feet.

The build is multi-arch from a single `docker buildx build --platform linux/arm64,linux/amd64 --push` invocation. One manifest list per tag points at the two arch-specific images. Consumers pull the appropriate arch transparently — `docker pull <docker-org>/phantom-service:0.1.0` on an arm64 host pulls the arm64 image; on an amd64 host pulls the amd64 image.

The base-image choice (Chainguard Wolfi) is **not** captured here — it is local to the Dockerfile (`src/phantom-service/docker/Dockerfile` and `src/phantom-emulator/src/phantom_emulator/docker/Dockerfile`) and the phantom README. Base-image substitutions are an implementation choice, not an architectural commitment.

The Dockerfiles live per-package:

- `src/phantom-service/docker/Dockerfile` — phantom-service.
- `src/phantom-emulator/src/phantom_emulator/docker/Dockerfile` — phantom-emulator.

Earlier Debian-slim placeholders at the repo root (`docker/Dockerfile`, `docker/docker-compose.yml`) are removed in this cycle (Phase 3 of `strategy_05_18.md`). The per-package homes match the workspace structure — each image's Dockerfile sits with the package it builds — and align with `tests/e2e/docker-compose.e2e.yml`'s existing `--file` references.

Status: Accepted
Date: 2026-05-20
