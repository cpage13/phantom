# 016. Phantom container deployment model

> **Superseded in part by ADR-020 on the registry and on how many images are published.** The registry is GHCR, not Docker Hub, and only `phantom-service` is a published artifact. The paragraph below is corrected in place rather than left standing, because it was contradicting both ADR-020 and this file's own Dockerfile list further down. The tag scheme and the multi-arch decision are unchanged and still current. See also the note at the end of this file on what is actually implemented.

Phantom self-builds and publishes one multi-arch (`linux/arm64` + `linux/amd64`) container image, `phantom-service` (the buffering upload-proxy). The registry is **GHCR**, `ghcr.io/<org>/phantom-service`, which is what `src/phantom-deploy/docker-compose.yml` and the deploy README both resolve. `phantom-emulator` is built for e2e and CI infrastructure and is never published. Consumers deploy by pulling the published tag; Phantom is **not** rebuilt by downstream consumers (notably a downstream Balena overlay does `image: ghcr.io/<org>/phantom-service:<tag>`, not `build: ...`).

Tag scheme:

- **`<version>`** — immutable, reproducible. The current cycle ships `0.1.0`. Versioned tags are what downstreams pin in production; once pushed, a versioned tag is never re-pushed.
- **`latest`** — floating; tracks the most recent stable build. Convenient for development and one-off smokes; **never pin `latest` in production deployments** because it moves under the consumer's feet.

The build is multi-arch from a single `docker buildx build --platform linux/arm64,linux/amd64 --push` invocation. One manifest list per tag points at the two arch-specific images. Consumers pull the appropriate arch transparently — `docker pull <docker-org>/phantom-service:0.1.0` on an arm64 host pulls the arm64 image; on an amd64 host pulls the amd64 image.

The base-image choice (Chainguard Wolfi) is **not** captured here — it is local to the Dockerfiles and the phantom README. Base-image substitutions are an implementation choice, not an architectural commitment.

The Dockerfiles live per-package (paths corrected 2026-07-15; ADR-020 consolidated the service image and the emulator image moved to its package root when the nested copy was retired):

- `src/phantom-deploy/Dockerfile` — phantom-service (see ADR-020).
- `src/phantom-emulator/Dockerfile` — phantom-emulator (e2e/CI infrastructure; never published).

Earlier Debian-slim placeholders at the repo root (`docker/Dockerfile`, `docker/docker-compose.yml`) are removed in this cycle. The stub-era `tests/e2e/docker-compose.e2e.yml` was retired 2026-07-15 in favor of the live docker-marked lane (`tests/e2e/docker/compose.yml` + `test_docker_volume_replacement.py`).

**Implementation status as of 2026-09-15.** The tag scheme above is not implemented. `.github/workflows/` contains exactly three workflows, `per_pr.yml`, `nightly_stress.yml` and `perf.yml`, and none of them builds, tags, signs or publishes an image. Nothing produces the versioned or `latest` tags this ADR describes, so there is no provenance on any artifact a consumer would pull and the image must currently be built locally. This paragraph records the gap rather than closing it; publishing is an outward-facing decision that belongs to the repository owner.

Status: Accepted
Date: 2026-05-20
