# `phantom-deploy` — Phantom service container image + reference compose

**What this is.** The container build configuration and the
reference docker-compose for `phantom-service`. **Not a Python
package** — this directory carries no `pyproject.toml`, no
`src/`, no Python code (per [ADR-020](../../docs/adr/020-container-image-as-deployment-artifact.md),
which supersedes the phantom-service portion of ADR-016).

**Audience.** Operators deploying Phantom. SDK consumers /
developers working on the Python code itself read the per-package
READMEs in `src/phantom*/`.

## Contents

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-arch (linux/amd64 + linux/arm64) Wolfi-based container image build. |
| `docker-compose.yml` | Reference compose for local dev + single-machine deployments. |
| `.dockerignore` | Build-context minimizer. |
| `README.md` | This file. |

## Build

```bash
# From the repository root.
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f src/phantom-deploy/Dockerfile \
  -t phantom-service:dev \
  .
```

The build is multi-arch via `docker buildx` — see the Docker docs
for the one-time `buildx create --use` setup if you have not used
buildx before.

The image is config-agnostic. The operator supplies the YAML config
at deploy time via a volume mount (`-v /etc/phantom/phantom.yaml:/etc/phantom/phantom.yaml`)
or `PHANTOM_*` env vars.

## Run

### Plain `docker run`

```bash
docker run \
  -v /var/lib/phantom:/var/lib/phantom \
  -v /etc/phantom/phantom.yaml:/etc/phantom/phantom.yaml \
  -e PHANTOM_SERVER__BIND_TCP=0.0.0.0:8080 \
  -p 127.0.0.1:8080:8080 \
  phantom-service:dev -c /etc/phantom/phantom.yaml
```

(The container listener must bind `0.0.0.0` so docker's port-forward
reaches it; the host side maps to `127.0.0.1` only, keeping the port
loopback-only on the machine. A bare-metal run uses the config's loopback
default with no override.)

### Compose

```bash
# From the repository root.
docker compose -f src/phantom-deploy/docker-compose.yml up
```

This pulls the published image (`ghcr.io/<org>/phantom-service:latest`)
by default. To build locally instead, uncomment the `build:` block
in `docker-compose.yml`.

## Configuration

The image takes its config from the YAML file passed to `-c`. See
[`config/phantom.yaml.example`](../../config/phantom.yaml.example)
for the full operator reference (every field annotated with
purpose + range + tradeoff).

Per-field overrides are also possible via `PHANTOM_*` env vars
(case-insensitive, `__` for nesting):

```bash
PHANTOM_OBSERVABILITY__LOG_LEVEL=DEBUG
PHANTOM_STORAGE__BODY_STORE__MODE=all_disk
PHANTOM_STORAGE__SQLITE__SYNCHRONOUS=FULL
```

Env-overlay covers scalar fields only; list-typed fields
(`instances`) live in YAML only.

## Hot reload

```bash
# Send SIGHUP to the running container's main process:
docker kill -s HUP phantom-service

# Or via the admin endpoint (on the host where the port is exposed):
curl -X POST http://127.0.0.1:8080/v1/admin/reload
```

See [ADR-013](../../docs/adr/013-hot-reload.md) for the list of
reloadable vs. restart-required knobs.

## Volumes

- `/var/lib/phantom` — the persistent data directory. Holds the
  SQLite DB(s), body files, optional cold-backup snapshots, and any
  flat timestamped quarantine backups. Mount a named volume or a host
  path; the container runs as the Wolfi `nonroot` user (UID/GID
  65532), and the data directory is created with that ownership
  at build time so a fresh named volume inherits it.

## Port

Phantom serves ONE listener in one process. The deployment is
same-machine-only, so intake, admin, and health all ride one socket; the
listener defaults to loopback (`127.0.0.1:8080`), and that loopback default
bind IS the admin access control (ADR-004).

- `8080` - the one listener: ingress (POST `/v1/send`), the admin surface
  (`/v1/admin/*`), and the liveness + readiness probes (`GET /v1/healthz`,
  `GET /v1/readyz`). The Dockerfile HEALTHCHECK probes `/v1/healthz` here.
  In a container the listener binds `0.0.0.0` (via
  `PHANTOM_SERVER__BIND_TCP`) so docker's port-forward reaches it, and the
  host-side mapping (`127.0.0.1:8080:8080`) keeps it loopback-only on the
  machine. A non-loopback bind exposes the unauthenticated admin API and
  warns at startup (front it with an authenticating reverse proxy).

## See also

- [Operator playbook](../../docs/operator-playbook.md) —
  deployment topology, mode selection, observability + alerting,
  failure-mode diagnosis, routine operations, YAML migration.
- [`config/phantom.yaml.example`](../../config/phantom.yaml.example)
  — operator-facing config reference.
- [ADR-020](../../docs/adr/020-container-image-as-deployment-artifact.md)
  — the deployment-shape decision.
- [ADR-016](../../docs/adr/016-phantom-container-deployment-model.md)
  — the superseded phantom-service portion of the prior container model.
