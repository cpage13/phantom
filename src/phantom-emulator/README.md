# `phantom-emulator`: generic two-step-upload emulator with failure injection

FastAPI service that imitates the protocol shape of a two-step file
upload (a metadata POST returning a presigned-style PUT URL, plus
an OAuth2 client-credentials JWT mint) and adds runtime-controllable
failure injection (5xx rate, latency, body cutoff, 401-after-N,
unavailable windows, approximate connection reset, presigned-URL
signature mismatch, TTL override, global pause).

**What it is.** A drop-in replacement for an upstream that speaks the
two-step-upload shape (metadata POST → presigned PUT), with a
`/control/*` surface for tests to deterministically inject failures.
It also provides the **fake-S3 SigV4 sink** and the **auth-free raw
sink** that back Phantom's fake-S3 routing E2E tests (the oracles for
re-signed and forward-as-is uploads).

**Audience.** Developers writing E2E tests for `phantom-service` (or
any service that talks to a similar two-step-upload upstream).
Publishable to public PyPI.

## Install

```bash
pip install phantom-emulator
# or
uv add phantom-emulator
```

Runtime requires Python 3.14+.

## Deployment modes

- **Wheel / in-process**: the dominant test mode.
  `await start_server(cfg)` returns a `Server` with `.url()`,
  `.stop()`, and a typed control surface (`inject_failure`, `pause`,
  `resume`, `expire_all_now`, `received`, …) that mirrors
  `/control/*` one-to-one.
- **Docker**: single-process uvicorn container for cross-process E2E.
  `python -m phantom_emulator -c /etc/phantom-emulator/config.yml`.

## Layout

```
src/phantom_emulator/
├── __init__.py            # public re-exports: AppConfig, start_server,
│                          # Server, FailurePolicy, AuthMode
├── __main__.py            # `python -m phantom_emulator` CLI
├── app.py                 # FastAPI factory + lifespan
├── server.py              # in-process Server + start_server
│                          # `Server.received()` returns entries carrying
│                          # body_hash (SHA-256 hex) + content_encoding
├── config.py              # AppConfig (Pydantic Settings) + YAML loader
│                          # S3Cfg: the fake-S3 sink's key-pair + body cap
├── state.py               # EmulatorState container
│                          # AcceptedBody carries content_encoding
│                          # S3Object / RawBody record the inbound .method
├── auth/
│   ├── jwt_minter.py      # HS256 / RS256 mint + verify
│   ├── jwks.py            # RSA keypair + JWKS doc
│   └── modes.py           # AuthMode enum + AuthModePolicy + authenticate
├── failure/
│   ├── injection.py       # FailurePolicy + FailureInjectionState
│   └── middleware.py      # per-scope failure injection
├── upload/
│   ├── presigned.py       # synthetic presigned-style URL mint
│   └── correlation.py     # metadata.keyValueStore extract / echo
├── routers/
│   ├── auth.py            # /oauth/token + /.well-known/*
│   ├── upstream.py        # /v1/files/create, /v1/files/upload/{token}
│   │                      # captures Content-Encoding on AcceptedBody
│   ├── control.py         # /control/* surface
│   │                      # ReceivedEntry shape: body_hash, content_encoding
│   ├── s3.py              # path-style fake-S3 sink (validates SigV4)
│   └── raw_sink.py        # auth-free /raw/{path} sink (forward-as-is)
└── docker/
    ├── Dockerfile
    └── compose.example.yml
```

## Two-step-upload endpoints

| Endpoint | Purpose |
|---|---|
| `POST /oauth/token` | OAuth2 client-credentials grant. Returns a Bearer JWT. |
| `GET /.well-known/openid-configuration` | OpenID Connect discovery doc. |
| `GET /.well-known/jwks.json` | JWKS doc (RS256 mode); empty in HS256. |
| `POST /v1/files/create` | Mint `FileInformation` + presigned PUT URL. Honors `Idempotency-Key`. Preserves `metadata.keyValueStore`. |
| `PUT /v1/files/upload/{token}` | Accept body bytes. Validates TTL and signature; records `x-amz-meta-*` headers and the `Content-Encoding` header. |
| `GET /v1/files/{file_id}` | Resolve a previously-minted `FileInformation`. |
| `POST /v1/files/search` | Stub returning `{"results": []}`. |

## Fake-S3 / raw sinks

The sinks back Phantom's fake-S3 routing E2E: a SigV4-validated path-style
sink (the oracle for `aws_sigv4` re-signed uploads) and an auth-free raw
sink (the oracle for `auth_mode: none` forward-as-is uploads). Both accept
the **full upload-verb set Phantom's catch-all forwards: `PUT`, `POST`,
and `PATCH`** (not PUT-only), with `GET` as the read-back. The stored
object records the inbound verb (`S3Object.method` / `RawBody.method`), so a
forwarded POST or PATCH validates and stores via the same method-agnostic
recompute as a PUT.

| Endpoint | Purpose |
|---|---|
| `PUT\|POST\|PATCH /{bucket}/{key}` | Path-style fake-S3 upload. Recomputes the inbound AWS SigV4 signature (enforcing `x-amz-content-sha256`): `200` + stores the body on match, `403 SignatureDoesNotMatch` on mismatch, `400` if `x-amz-content-sha256` is missing, `413` over the body cap. |
| `GET /{bucket}/{key}` | SigV4-validated read-back of the stored bytes (`404 NoSuchKey` if absent). |
| `PUT\|POST\|PATCH /raw/{path}` | Auth-free, token-free upload: `200` + stores the bytes keyed by the full forwarded path. No signature, no token. |
| `GET /raw/{path}` | Auth-free read-back of the stored bytes. |

No full S3: no multipart / list / ACL / tagging / versioning / copy, and no
bare-bucket `CreateBucket` (a path-shape gap, not a verb gap: the sink
requires a `{bucket}/{key}` path).

## Control surface

| Endpoint | Effect |
|---|---|
| `GET /control/status` | Snapshot (uptime, counts, policies, auth-mode, paused flag). |
| `GET /control/received` | In-memory log of accepted upload bodies. Each entry carries `body_hash` (SHA-256 hex of received bytes) and `content_encoding`. |
| `POST /control/inject-failure` | Install / replace a `FailurePolicy`. |
| `POST /control/clear-failures` | Drop every installed policy. |
| `POST /control/pause` / `resume` | Toggle global upstream 503. |
| `POST /control/shutdown` | SIGTERM self (Docker mode only). |
| `POST /control/expire-all-now` | Age every issued JWT past `exp`. |
| `POST /control/revoke-tokens` | Drop every issued JWT. |
| `POST /control/auth/extra-claims` | Stage extra claims for the next mint. |
| `POST /control/auth/mode` | Swap default auth mode (`*`) or per-path override. |
| `POST /control/presigned-ttl` | Set the presigned URL TTL for new mints. |
| `POST /control/seed` | Reseed the failure-injection RNG. |
| `POST /control/clear-received` | Drop the accepted-bodies log. |

## YAML configuration

An empty YAML produces a working emulator. Override any value via
`PHANTOM_EMULATOR_<UPPER_SNAKE_DOTTED>` env vars
(`PHANTOM_EMULATOR_AUTH__SIGNING__MODE=RS256`).

```yaml
server:
  host: "0.0.0.0"
  port: 8000

auth:
  default_mode: "oauth_client_credentials"
  signing:
    mode: "HS256"
    hs256_secret_env: "EMULATOR_SIGNING_KEY"
    rs256_keypair: "auto"
  issuer: "http://emulator"
  audience: "api://emulator/.default"
  default_expires_in_seconds: 3600
  clock_skew_seconds: 300
  tenant_id: "00000000-0000-0000-0000-000000000000"
  clients:
    - client_id: "test-client"
      client_secret: "test-secret"

upstream:
  presigned_ttl_seconds: 3600
  body_max_bytes: 2147483648
  idempotency_dedup_window_seconds: 3600

failure_injection: {}

control:
  bind: "loopback"

logging:
  level: "INFO"
```

## Error model

The emulator returns the same `ErrorEnvelope` shape Phantom does
where it makes sense (5xx responses inject the configured failure
policy's body if any), but is otherwise a vanilla FastAPI service
returning 200 / 400 / 401 / 403 / 404 / 503 per the upstream contract
it imitates. Test code typically asserts against `Server.received()`
entries + the upstream response status. Failure injection is the
primary surface, not error-model parity.

## Running tests

```bash
# From the workspace root:
uv run pytest src/phantom-emulator/tests
```

`mypy --strict` clean. `ruff check` + `ruff format --check` clean.
Includes per-module unit tests plus an end-to-end smoke test that
boots the emulator on an ephemeral port and exercises the full
mint → create → PUT → GET → received flow.
