# `phantom`: the buffering upload-proxy service

FastAPI + asyncio service. Single Python process per host. Generic
buffering HTTP proxy with **zero upstream-specific knowledge**. It owns
the wire protocol, every model, every header, every error code.

**What it is.** Phantom accepts a multi-step HTTP "chain envelope"
(`ChainEnvelope` per ADR-009/010), acks fast with HTTP 202 + a
synthetic `ChainResponse`, persists the body locally (RAM, disk, or
a hybrid of the two; the operator chooses), and runs the actual
upstream calls in the background with retries and token refresh.

**Audience.** Operators deploying Phantom on producers and developers
working on the service itself. SDK integrators read the
[`phantom-client` README](../phantom-client/README.md) instead.

**Distribution.** Two artifacts:

- **Container image** (the production artifact): single Wolfi
  multi-arch image at `src/phantom-deploy/Dockerfile`. See
  [ADR-020](../../docs/adr/020-container-image-as-deployment-artifact.md).
- **PyPI wheel**: `phantom-service` (the import name `phantom` was
  already taken). Suitable for in-process embedding or local dev.

## Install / build / run

```bash
# From the workspace root. Local dev:
uv sync --all-packages
cp config/phantom.yaml.example config/phantom.yaml   # first run only
uv run python -m phantom --config config/phantom.yaml

# Container build (multi-arch):
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f src/phantom-deploy/Dockerfile \
  -t phantom-service:dev \
  .

# Container run:
docker run \
  -v /var/lib/phantom:/var/lib/phantom \
  -v $(pwd)/config/phantom.yaml:/etc/phantom/phantom.yaml \
  phantom-service:dev -c /etc/phantom/phantom.yaml
```

See [`config/phantom.yaml.example`](../../config/phantom.yaml.example)
for the full operator-facing configuration reference.

## Layout

```
src/phantom/
├── __init__.py
├── __main__.py            # `python -m phantom --config <path>`
├── app.py                 # FastAPI factory + composition root; its lifespan
│                          # is the SOLE site that spawns every long-lived
│                          # coroutine, under one asyncio.TaskGroup; runs the
│                          # boot guards (startup_checks) before workers start
├── runtime/
│   ├── lock_retry.py      # bounded retry-with-backoff for transient SQLite
│   │                      # locks at boot (recovery writes + database open)
│   ├── reload.py          # hot-reload engine (SIGHUP + POST /v1/admin/reload
│   │                      # both funnel through apply_reload)
│   ├── startup_checks.py  # boot-time guards the lifespan runs (umask,
│   │                      # retention-floor, instance-isolation, all_ram mode
│   │                      # guard, Phase 4 integrity gate) + the one shared
│   │                      # build_body_store mode table
│   └── tls_cert.py        # startup TLS cert lifecycle (operator-supplied PEM
│                          # pair or auto-gen self-signed; resolve_tls_paths)
├── models/                # Pydantic v2 wire and persistence types
│                          # (ChainEnvelope family; UploadRow with body_location
│                          # column + body_hashes; ErrorBody / ErrorEnvelope;
│                          # ChainAdminDetail; TokenSlot)
├── config/                # Settings (pydantic-settings)
│   ├── settings.py        #   reload_from_yaml(path, *, skip_probe=...)
│   ├── probe.py           #   MachineFacts + psutil/shutil probe
│   ├── defaults.py        #   ResolvedDefaults from probe
│   └── ad_mint.py         #   typed AdMintConfig
├── storage/               # SQLite stores: uploads.db + token_cache.db
│                          # + credential_store.db (ADR-030)
│   ├── interface.py       #   UploadStore / BodyStore / TokenCache Protocols
│   ├── sqlite_store.py    #   SqliteUploadStore (insert_with_idempotency_claim
│   │                      #   is the atomic admission writer per ADR-019)
│   ├── token_cache.py     #   SqliteTokenCache (own DB file: token_cache.db)
│   ├── credential_store.py #   host-keyed destination SigV4 credential store
│   │                      #   (own DB file: credential_store.db)
│   ├── ram_body_store.py  #   RamBodyStore
│   ├── file_body_store.py #   FileBodyStore (atomic rename + fsync file + parent)
│   ├── hybrid_body_store.py #   HybridBodyStore (Ram+File composition)
│   ├── integrity.py       #   Phase 4 PRAGMA integrity_check + quarantine
│   ├── timestamps.py      #   UTC Z-suffix timestamps for backup + quarantine
│   │                      #   artifact names
│   ├── errors.py          #   StorageCorruptionError, CodecRoundTripDriftError,
│   │                      #   BodyMissingError
│   └── schema.sql         #   body_location ENUM('ram','file') + body_hashes_json
├── compression/           # BodyCodec Protocol + zstd / gzip / passthrough
├── chain/                 # JSONPath wrapper, envelope parser, ChainExecutor
│                          # (discriminated-union result types; exhaustive by mypy)
│                          # sigv4_signer.py: the aws_sigv4 re-sign arm
│                          # (SigningService -> botocore signer dispatch)
├── transport/             # UpstreamClient Protocol + httpx impl
├── refresh/               # AdMinter, supervised by app.py's lifespan TaskGroup (Phase 2 H6)
├── strategies/            # UploadStrategy Protocol + two retry schedulers
├── routing/               # resolve_route(url, instance_cfg) function
│                          # auth_mode is 3-valued: phantom_bearer / none / aws_sigv4
├── instances/             # InstanceContext / InstanceDispatcher /
│                          # InstanceSettingsSnapshot / SettingsHolder
├── workers/               # All supervised by app.py's lifespan TaskGroup:
│                          #   Sender, Reaper, AuthKicker (bearer), VacuumScheduler,
│                          #   CredentialKicker (credential_kicker.py: wakes
│                          #   auth_expired aws_sigv4 rows on a fresh cred push),
│                          #   SaturationGate, PersistController (sole
│                          #   body_location='file' writer per invariant #6),
│                          #   RamPressureWatcher, BodyOrphanJanitor,
│                          #   DiskPressureProbe, InvariantAuditor (Phase 3),
│                          #   ColdBackupScheduler (Phase 4 optional),
│                          #   run_recovery (boot sweep, two actions: reset
│                          #   attempting to queued; quarantine missing-body rows)
├── observability/         # logging.py (bearer redaction; SensitiveCaptureRedactor)
│                          # metrics.py (Phase 3: in-process counter + gauge
│                          # registry surfaced via /v1/admin/observability/*)
└── routes/
    ├── send.py            # POST /v1/send (under 100 lines;
    │                      # check_post_send_size.py asserts)
    ├── admission.py       # admit_chain: atomic admission via
    │                      # insert_with_idempotency_claim (ADR-019, Phase 1 H7)
    ├── admin.py           # GET/POST/PUT/DELETE /v1/admin/*
    ├── health.py          # GET /v1/healthz (liveness) + GET /v1/readyz (readiness)
    ├── catch_all.py       # PUT|POST|PATCH /{phantom_path:path} raw-intake /
    │                      # fake-S3 (stock-SDK ingress; ?phantom= / default target)
    ├── _version.py        # API_VERSION "v1": single source of the /v1 router prefixes
    └── envelope.py
```

## Public surface

The HTTP surface IS the public surface. No Python API is exported
for in-process consumers (use `phantom-client` for that).

### The single listener (`bind_tcp`, default `127.0.0.1:8080`)

The deployment is same-machine-only, so ONE listener serves intake, admin,
and health on one socket (loopback by default). Set `server.tls.enabled:
true` to flip that same listener to HTTPS (no second socket). It uses an
auto-gen self-signed cert or an operator-supplied PEM pair.

| Endpoint | Purpose |
|---|---|
| `POST /v1/send` | Submit a `ChainEnvelope` (JSON or multipart). 202 + `ChainResponse` + `X-Phantom-*` headers; 200 on idempotency replay. |
| `GET /v1/healthz` | Liveness probe (the process is up). |
| `GET /v1/readyz` | Readiness probe (every instance's DB is open + writable). |

### Admin (loopback by default per [ADR-004](../../docs/adr/004-admin-api-loopback-no-auth.md))

The admin router rides the SAME single listener (`bind_tcp` else
`bind_uds`, default `127.0.0.1:8080`). The loopback default bind IS the
admin access control: nothing is reachable off-box by default, so these
endpoints are not either. A non-loopback `bind_tcp` is a deliberate opt-in
that warns at startup (admin rides this listener and is unauthenticated).
The bind knobs are restart-required.

| Endpoint | Purpose |
|---|---|
| `GET /v1/admin/status` | Aggregate process status: resolved defaults, observed machine facts. |
| `GET /v1/admin/stats` | Aggregate counts (per-state row tallies). |
| `GET /v1/admin/instances` | List configured instances + per-instance summary. |
| `GET /v1/admin/instances/{id}/status` | Per-instance status. |
| `GET /v1/admin/chains` | Paginated list with `ExtractFilter`/`DeleteFilter`/`KeyValueMatchFilter`. |
| `GET /v1/admin/chains/{chain_id}` | `ChainAdminDetail` - state, body_location, captured values, attempts, last_error. |
| `GET /v1/admin/chains/{chain_id}/body` | Stream one chain's buffered body. |
| `GET /v1/admin/chains/{chain_id}/bundle` | Stream one chain's body + metadata bundle. |
| `GET /v1/admin/groups/{group_id}` | Group status (multifile grouping; ADR-028). |
| `GET /v1/admin/uploads/by-captured-id/{id}` | Reverse lookup by upstream-captured id (per-instance `admin_lookup`). |
| `GET /v1/admin/uploads/by-local-uuid/{id}` | Reverse lookup by local chain uuid. |
| `POST /v1/admin/chains/extract` | Filtered extract tar (`since`/`chain_ids`; ADR-005) - distinct from `export.tar`. |
| `GET /v1/admin/export.tar` | Bulk export - streams every buffered body + manifest.json. ADR-005. |
| `POST /v1/admin/chains/{chain_id}/replay` | Re-queue a terminal chain for another delivery attempt. |
| `POST /v1/admin/chains/{chain_id}/cancel` | Cancel a single (non-terminal) chain → `cancelled`. |
| `DELETE /v1/admin/chains/{chain_id}` | Hard delete one chain + its body. |
| `DELETE /v1/admin/chains` | Bulk delete by filter (rejects empty filters; C1 closure includes body-file deletion). |
| `GET /v1/admin/tokens` | List token slots, optionally filtered by `?endpoint=` (no bearer values returned - ADR-004). |
| `PUT /v1/admin/tokens*` | Push a token into the cache (per-slot, per-endpoint, or all; admin override). |
| `DELETE /v1/admin/tokens*` | Token cache invalidation (per-slot or all): slots are marked `bad` and preserved (ADR-003), not deleted. |
| `PUT /v1/admin/credentials/{dest_host}` | Push a destination SigV4 credential into the host-keyed store (per-host; resolved literals; `204`, secret never echoed). No GET/LIST credential endpoint exists (unlike tokens). |
| `GET /v1/admin/observability/counters` | All counters from MetricsRegistry (Phase 3). |
| `GET /v1/admin/observability/gauges` | All gauges. |
| `GET /v1/admin/observability/ram_pressure` | RAM-pressure snapshot. |
| `GET /v1/admin/quarantine` | Inventory of past corruption-event quarantine backups (Phase 4). |
| `POST /v1/admin/quarantine/restore` | Restore a quarantined backup by `backup_id` (ADR-025/026). |
| `POST /v1/admin/reload` | Trigger hot reload from YAML (ADR-013). |

## Hot reload

```bash
kill -HUP <pid>
# or
curl -X POST http://127.0.0.1:8080/v1/admin/reload
```

See [ADR-013](../../docs/adr/013-hot-reload.md) for the list of
reloadable vs. restart-required knobs.

## Error model

Phantom emits a stable `ErrorEnvelope` body shape on every error
response. See [ADR-017 error-code matrix](../../docs/adr/017-error-code-matrix.md)
for the authoritative table of every HTTP status × `error.code`
pair, their `details` payloads, and the typed `phantom-client`
exception class for each.

## Tests

```bash
# From the workspace root:
uv run pytest src/phantom-service/tests
```

Per-package unit + integration tests. `mypy --strict` clean
(workspace mypy via `bash scripts/precommit/run_mypy_per_package.sh`).
`ruff check` + `ruff format --check` clean.

## Falsifiability tooling

Run from the workspace root:

```bash
uv run scripts/check_post_send_size.py        # post_send <= 100 lines
uv run scripts/check_descriptions.py          # every Pydantic field has description
uv run scripts/check_persist_ordering.py      # fsync precedes body_location='file' flip
uv run scripts/check_atomic_admission.py      # admission uses insert_with_idempotency_claim
```

## See also

- [docs/architecture-intent.md](../../docs/architecture-intent.md): onboarding map.
- [docs/operator-playbook.md](../../docs/operator-playbook.md): deployment + recovery.
- [CONTEXT.md](../../CONTEXT.md): domain glossary.
- [docs/adr/](../../docs/adr/): settled decisions.
