# Phantom

**Phantom is a small upload-buffering service you run alongside your data-producing process.** Your code uploads to Phantom instead of to the cloud. Phantom acknowledges in milliseconds, persists the bytes locally, and delivers the upload in the background: it retries through outages, refreshes bearer tokens, and can re-sign uploads with AWS SigV4 so a stock S3 SDK works with no Phantom-specific code. It is built to survive crashes, power loss, disk and memory pressure, and expired credentials without losing data or sending duplicates, and the end-to-end test suite injects those exact failures to prove it.

## The pieces

There are four pieces. Most deployments touch only two of them.

| Piece | What it is | Who installs it |
|---|---|---|
| **`phantom`** | The service. A FastAPI process that accepts uploads, persists them, retries them upstream. Runs as a container. | Operators |
| **`phantom-client`** | The Python SDK for a running Phantom. `await client.submit_chain(...)` and you are done. | Anyone writing producer code |
| **`phantom-emulator`** | A fake upstream with controllable failure injection. Lets you test end-to-end with no internet. | Anyone writing tests against Phantom |
| **`phantom-deploy`** | The Dockerfile and a reference `docker-compose.yml`. The image is the deployment artifact. | Operators |

## Run Phantom

```bash
# 1. Copy the example config; set your destination and auth.
cp config/phantom.yaml.example phantom.yaml
$EDITOR phantom.yaml

# 2. Build the image and run it with the config mounted.
#    In a container, bind 0.0.0.0 so docker's port-forward reaches it;
#    the host-side 127.0.0.1 mapping keeps the port loopback-only.
docker buildx build -f src/phantom-deploy/Dockerfile -t phantom-service:dev .
docker run --rm \
    -e PHANTOM_SERVER__BIND_TCP=0.0.0.0:8080 \
    -p 127.0.0.1:8080:8080 \
    -v $PWD/phantom.yaml:/etc/phantom/phantom.yaml:ro \
    -v phantom-data:/var/lib/phantom \
    phantom-service:dev -c /etc/phantom/phantom.yaml

# 3. Verify it is up.
curl http://localhost:8080/v1/healthz
```

That is the whole deploy. Everything (ingress, admin, health) rides the one port. Ingress is plain HTTP by default, which is the right posture for the same-host loopback deployment. To serve HTTPS (enable it when exposing Phantom on a network), set `server.tls.enabled: true`; Phantom auto-generates a self-signed cert, or you supply your own PEM pair. Deployment, tuning, and monitoring live in the [operator playbook](docs/operator-playbook.md); a compose file lives at [src/phantom-deploy/docker-compose.yml](src/phantom-deploy/docker-compose.yml).

## Use it in three moves

### 1. Point your uploader at Phantom

With the Python SDK (lives at [src/phantom-client](src/phantom-client/README.md), installable with `pip install ./src/phantom-client`, Python 3.12+):

```python
from uuid import uuid4
from phantom_client import ChainBodyJson, ChainEnvelope, ChainStep, PhantomClient

async with PhantomClient("http://localhost:8080") as client:
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        steps=[
            ChainStep(
                name="create",
                method="POST",
                url="https://files.upstream.example/v1/files/create",
                body=ChainBodyJson(value={"name": "reading.dat"}),
            ),
        ],
    )
    await client.submit_chain(envelope, uid="some-uid")   # returns in milliseconds
    final = await client.poll_until(chain_id)
    print(final.state)   # "succeeded" / "failed" / "stored" / ...
```

Or skip the SDK entirely: any stock S3 SDK pointed at Phantom works for S3 destinations (quick start below).

### 2. Set the destination

The SDK envelope names its destination in each step's `url`; Phantom routes by the first step's URL. Raw S3 uploads resolve the destination from an explicit `?phantom=<url>` query carrier on the request, or the configured `phantom_default_target`; the carrier wins. With neither, the request is rejected 421 before anything is stored.

### 3. Provide the credential Phantom should use

Each route's `auth_mode` (in `phantom.yaml`) says what Phantom does at forward time:

- **`phantom_bearer`**: pass the token on the upload, `await client.submit_chain(envelope, auth_token="<bearer>")`. Phantom caches it per `(endpoint, uid)` and reuses it for retries; push a fresh one any time with `await client.push_token(endpoint=..., uid=..., token=...)`. Or configure `ad_mint` and Phantom mints Azure AD client-credentials tokens itself.
- **`aws_sigv4`**: push the destination credential once; Phantom re-signs every upload with it:

  ```python
  from phantom_client import SigningService, SigV4StaticCredBody

  await client.push_credential(
      dest_host="s3.amazonaws.com",
      credential=SigV4StaticCredBody(
          access_key_id="AKIA...",
          secret_access_key="...",
          region="us-east-1",
          service=SigningService.S3,
      ),
  )
  ```

  Or provision it at boot in `phantom.yaml` (`sigv4_credentials`, env-var names only), or `curl -X PUT` the admin endpoint. All three forms are in the [operator playbook](docs/operator-playbook.md).
- **`none`**: nothing to provide. The request's own auth (a presigned URL, for example) is forwarded as-is.

## Quick start: upload with a stock S3 SDK (no phantom-client)

If your destination is AWS S3, you do not need `phantom-client` at all. Point a stock S3 SDK at Phantom over plain HTTP, path-style, and Phantom buffers the object and re-signs it for the real bucket:

```python
import boto3
from botocore.config import Config

# Point boto3 at Phantom (plain HTTP, path-style). Credentials here are
# placeholders: Phantom RE-SIGNS the request for the real bucket with the
# credentials an operator pushed to it. These are not the AWS keys that
# reach S3.
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8080",
    aws_access_key_id="phantom", aws_secret_access_key="phantom",
    config=Config(s3={"addressing_style": "path"}),
)
s3.put_object(Bucket="my-bucket", Key="readings/2026-06-26.dat", Body=b"...")
# Phantom acks fast, buffers, and forwards to the real upstream in the
# background. Destination: phantom_default_target or ?phantom=<url>.
```

Pushing the SigV4 credential and choosing the route's `auth_mode`: see the [operator playbook](docs/operator-playbook.md).

## Going deeper

- **Deploy, tune, monitor in production** → [docs/operator-playbook.md](docs/operator-playbook.md)
- **Write code against Phantom** → [src/phantom-client/README.md](src/phantom-client/README.md)
- **Test without internet** → [src/phantom-emulator/README.md](src/phantom-emulator/README.md)
- **Container build + compose** → [src/phantom-deploy/README.md](src/phantom-deploy/README.md)
- **How the service works inside** → [docs/architecture-intent.md](docs/architecture-intent.md)
- **Request lifecycle, storage design, DB retry layers** → [docs/engineering/architecture.md](docs/engineering/architecture.md)
- **Error contract, failure modes, security posture** → [docs/engineering/reliability-and-security.md](docs/engineering/reliability-and-security.md)
- **The test suite that proves the durability** → [docs/engineering/test-suite.md](docs/engineering/test-suite.md)
- **Why decisions were made** → [docs/adr/](docs/adr/)
- **Domain glossary (read first when contributing)** → [CONTEXT.md](CONTEXT.md)

## For contributors

```bash
uv sync --all-packages                           # install the workspace (plain `uv sync` installs no members)
uv run pytest src/phantom-service/tests          # service unit lane
uv run pytest src/phantom-client/tests           # client unit lane
uv run pytest src/phantom-emulator/tests         # emulator unit lane
uv run pytest tests                              # workspace lane: contract + integration + e2e (minutes, not seconds)
bash scripts/precommit/run_mypy_per_package.sh   # strict mypy per package
uv run pre-commit run --all-files                # ruff + falsifiability checks
```

The four pytest lanes run separately, from the repo root. A single bare `uv run pytest` does not collect: the three per-package `tests` packages and the workspace `tests` namespace package share one name, so one process cannot import them all.

Stack: Python 3.14 (service, emulator, workspace), 3.12+ (client SDK), `uv`, FastAPI, Pydantic v2, Wolfi-based container image.

Linux is the production target. macOS and Windows are dev-only; macOS `os.fsync` does not give the strict durability the persistence layer assumes.

## License

Released under the [MIT License](LICENSE).
