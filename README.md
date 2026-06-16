# Phantom

**Phantom is a small upload-buffering service you run alongside your data-producing process.** Your code POSTs files to Phantom instead of to the cloud directly. Phantom acknowledges immediately, stores the bytes locally, and handles the real upload to the cloud in the background — retrying on failure, refreshing auth tokens, and never losing data if the cloud goes away for a while.

Your process gets a fast, reliable upload endpoint that always works. Phantom worries about the network.

## The problem it solves

Producers, field-deployed sensors, and other data-producing processes typically have two problems with cloud uploads:

1. **The network is unreliable.** A pi-class producer running long upload sequences cannot tolerate a 30-second blocking POST every time the upstream API hiccups.
2. **Auth is annoying.** Bearer tokens expire, refresh flows have edge cases, and bolting that logic into every producer codebase is repetitive and error-prone.

Phantom takes both concerns off the producer:

- Uploads to Phantom return in milliseconds with HTTP 202 and a tracking ID. The producer moves on.
- Phantom persists every body to local disk or RAM before acknowledging, so a crash doesn't lose work.
- A background sender does the actual cloud upload, retrying with backoff until it succeeds or the producer operator marks the upload terminal.
- Token refresh runs in the background. Producers that push their own bearer tokens still work; producers that want Phantom to mint Azure AD client-credentials tokens get that for free.

## The pieces

There are four pieces. You probably only need two of them.

| Piece | What it is | Who installs it |
|---|---|---|
| **`phantom`** | The service itself. A FastAPI process that accepts uploads, persists them, retries them upstream. Runs as a container. | Operators |
| **`phantom-client`** | A Python SDK that talks to a running Phantom instance. `await client.send(...)` and you're done. | Anyone writing producer code |
| **`phantom-emulator`** | A fake upstream that pretends to be the real cloud API. Lets you write end-to-end tests with no internet. | Anyone writing tests against Phantom |
| **`phantom-deploy`** | The Dockerfile and a reference `docker-compose.yml`. The image is the deployment artifact. | Operators |

Most deployments only touch `phantom-deploy` (to get the container running) and `phantom-client` (to talk to it).

## How they fit together

```
   ┌─────────────────┐                    ┌─────────────────┐
   │ Your producer   │  phantom-client    │  Phantom        │
   │ (device, sensor,│ ─────send()──────▶ │  (container on  │
   │  service)       │ ◀─── HTTP 202 ──── │   the same host)│
   └─────────────────┘                    └────────┬────────┘
                                                   │
                                       persist body to
                                       RAM and/or disk
                                                   │
                                          background sender
                                          retry + token refresh
                                                   │
                                                   ▼
                                          ┌─────────────────┐
                                          │ Real cloud      │
                                          │ upstream API    │
                                          └─────────────────┘
```

The producer never sees the cloud directly. Phantom is the only thing that talks upstream. If the cloud is unreachable, Phantom keeps the body locally and keeps retrying; the producer is unaware.

For local development and CI, replace the real cloud with `phantom-emulator` — same protocol, runs on your laptop, supports failure injection so you can test retry behavior deterministically.

## Quick start — deploy Phantom

The minimum operator path:

```bash
# 1. Copy the example config and set your upstream URL + auth.
cp config/phantom.yaml.example phantom.yaml
$EDITOR phantom.yaml

# 2. Run the container with the config mounted.
#    Images publish to BOTH GHCR and Docker Hub on every release —
#    pull from whichever you prefer.
#    The single listener (intake + admin + health) defaults to loopback;
#    in a container bind 0.0.0.0 so docker's port-forward reaches it, and
#    map the host side to 127.0.0.1 to keep it loopback-only on the machine.
docker run --rm \
    -e PHANTOM_SERVER__BIND_TCP=0.0.0.0:8080 \
    -p 127.0.0.1:8080:8080 \
    -v $PWD/phantom.yaml:/etc/phantom/phantom.yaml:ro \
    -v phantom-data:/var/lib/phantom \
    docker.io/<org>/phantom-service:<tag>
    # or: ghcr.io/<org>/phantom-service:<tag>

# 3. Verify it's up.
curl http://localhost:8080/v1/healthz
```

That's the whole deploy. Everything (ingress, admin, health) rides the one
loopback port. Tune from there using the operator playbook (link below).

For a `docker-compose.yml` example see `src/phantom-deploy/docker-compose.yml`.

## Quick start — talk to Phantom from your code

```python
import asyncio
from phantom_client import PhantomClient

async def main():
    async with PhantomClient("http://localhost:8080") as client:
        result = await client.send(
            endpoint="files",
            body=b"<file bytes here>",
            metadata={"ref_id": "12345"},
        )
        print(result.upload_id, result.status)

asyncio.run(main())
```

Install:

```bash
pip install phantom-client    # or: uv add phantom-client
```

## Configuration at a glance

Three deployment modes; pick one in `phantom.yaml` under `storage.body_store.mode`:

| Mode | Where bodies live | When to choose it |
|---|---|---|
| **`hybrid`** *(default)* | RAM first; spilled to disk on memory pressure or retry-linger | Most production deployments |
| **`all_disk`** | Every body written to disk immediately | Durability-critical: you cannot lose anything across a power cycle |
| **`all_ram`** | Bodies stay in RAM, never written to disk | Ephemeral: workloads where re-running on data loss is fine, you want lowest latency |

Every other knob has a sensible default. See the operator playbook for tuning.

## Built to be trusted

Phantom is designed to survive crashes, power loss, disk and memory pressure, upstream outages, expired credentials, and database lock contention without losing data and without sending duplicates. That is where most of the engineering effort goes, and it is backed by an end-to-end test suite that injects those exact failures and asserts correct behavior anyway.

For the full breakdown of how, three documents go deep:

- **Architecture and operational framework**: how a request flows, how data is stored, and the layered database retry mechanisms that keep storage correct under contention. [docs/engineering/architecture.md](docs/engineering/architecture.md)
- **The end-to-end test suite**: the functional, performance, aggressor, and reliability tests that prove the durability is real. [docs/engineering/test-suite.md](docs/engineering/test-suite.md)
- **Reliability, error handling, and security**: the error-code contract, the fallback procedure for every failure mode, the security posture, and the robustness guarantees. [docs/engineering/reliability-and-security.md](docs/engineering/reliability-and-security.md)

## Where to go next

- **Deploy + tune + monitor in production** → [docs/operator-playbook.md](docs/operator-playbook.md)
- **Write code against Phantom** → [src/phantom-client/README.md](src/phantom-client/README.md)
- **Run tests without internet** → [src/phantom-emulator/README.md](src/phantom-emulator/README.md)
- **Container build + compose** → [src/phantom-deploy/README.md](src/phantom-deploy/README.md)
- **Understand how the service works internally** → [docs/architecture-intent.md](docs/architecture-intent.md)
- **See why a decision was made the way it was** → [docs/adr/](docs/adr/)

## For contributors

Working setup:

```bash
uv sync                                          # install workspace
uv run pytest                                    # per-package + workspace tests
bash scripts/precommit/run_mypy_per_package.sh   # strict mypy per package
uv run pre-commit run --all-files                # ruff + falsifiability checks
```

- **Code style and conventions** → [CONTEXT.md](CONTEXT.md) (domain glossary, read first)
- **Architecture and invariants** → [docs/architecture-intent.md](docs/architecture-intent.md)
- **Test layers + falsifiability scripts** → [docs/architecture-intent.md § 9](docs/architecture-intent.md)

Stack: Python 3.14 service / 3.12 SDKs, `uv`, FastAPI, Pydantic v2, Wolfi-based container image.

Linux is the production target. macOS and Windows are dev-only — macOS's `os.fsync` does not strictly fsync on HFS+.

## License

Released under the [MIT License](LICENSE).
