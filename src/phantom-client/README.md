# `phantom-client`: generic Python SDK for Phantom

A thin async HTTP client over Phantom-the-service's `POST /v1/send`
ingress, the `X-Phantom-*` response headers, the admin API surface
(chains, tokens, destination SigV4 credentials, status, export.tar,
observability, quarantine), and the chain-status poller.

**What it is.** A typed Python facade so applications can submit
chain envelopes and observe Phantom's admin surface without
hand-rolling httpx + ADR-010 model duplication.

**Audience.** Integrators building applications on top of Phantom.
This package has no organization-internal dependencies. Runtime
deps are exactly `httpx>=0.23.3,<0.24` and `pydantic>=2`. Publishable
to public PyPI.

## Install

```bash
# From the repo root:
pip install ./src/phantom-client
```

The package is not yet published to public PyPI.

Runtime requires Python 3.12+.

## Quick start

```python
from uuid import uuid4
from phantom_client import PhantomClient, ChainEnvelope, ChainStep, ChainBodyJson

async with PhantomClient("http://phantom:8080") as client:
    chain_id = uuid4()
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[
            ChainStep(
                name="example",
                method="POST",
                url="https://example.com/things",
                body=ChainBodyJson(value={"hello": "world"}),
            ),
        ],
    )
    response = await client.submit_chain(envelope, uid="some-uid")
    final = await client.poll_until(chain_id)
    print(final.state)  # "succeeded" / "failed" / "stored" / "corrupted" / ...
```

Phantom routes each submission by the first step's URL; there is no
separate target header.

### Groups: submit several uploads, wait for the whole set

Tag submissions with a shared `group_id` (`SubmitOptions`, emitted as
`X-Phantom-Group-Id`), then wait on the set with one call. `multifile_id`
and `order` record multi-file association and position (display only,
never enforced at delivery); both tags are optional and independent.

```python
from uuid import uuid4
from phantom_client import PhantomClient, SubmitOptions

group_id = uuid4()

async with PhantomClient("http://phantom:8080") as client:
    for position, envelope in enumerate(envelopes):  # built as above
        await client.submit_chain(
            envelope,
            uid="some-uid",
            options=SubmitOptions(group_id=group_id, order=position),
        )

    rollup = await client.poll_group_until_finished(group_id)
    print(rollup.all_finished)     # True (that's the stop condition)
    print(rollup.counts_by_state)  # {"succeeded": 3, "queued": 0, ...}
    for member in rollup.members:
        print(member.chain_id, member.state, member.sent_at)
```

`all_finished` is true once no member is `queued` or `attempting`
(`auth_expired` and `corrupted` count as finished: neither progresses
without intervention). `get_group_status(group_id)` is the one-shot,
non-polling form. Every upload is a group of one by default (`group_id`
falls back to `chain_id` server-side), so `get_group_status(chain_id)`
works on optionless submissions too.

### Find an upload by either identifier

```python
# By the producer-minted local uuid (metadata key pinned server-side):
result = await client.find_by_local_uuid(local_uuid)

# By the upstream-assigned id captured from the create step (the
# instance's admin_lookup config supplies where that id lives):
result = await client.find_by_captured_id("some-upstream-file-id")

if result.found:
    print(result.matches[0].state, result.matches[0].sent_at)
```

A miss is a normal `found=False` answer (HTTP 200), not an exception;
only the group rollup 404s on an unknown id.

## Layout

```
src/phantom_client/
├── __init__.py            # public re-exports
├── client.py              # PhantomClient async facade
│                          # `get_upload` and `poll_until` return ChainAdminDetail;
│                          # group + identifier reads: get_group_status,
│                          # find_by_local_uuid, find_by_captured_id,
│                          # poll_group_until_finished
├── transport.py           # single HTTP source of truth (httpx.AsyncClient)
│                          # `_build_multipart` assigns a non-empty filename
│                          # to every part: load-bearing for the
│                          # transparent-proxy invariant on bodies with
│                          # bytes >= 0x80
├── headers.py             # X-Phantom-* constants + build_request_headers
├── config.py              # ClientConfig, Timeouts, RetryPolicy, SubmitOptions
├── errors.py              # exception hierarchy + EXCEPTION_FOR_CODE mapping
├── poller.py              # poll_until + poll_group_until_finished helpers
│                          # (ChainAdminDetail / GroupStatusResponse)
└── models/
    ├── chain.py           # ADR-010 envelope shapes (byte-identical to
    │                      # phantom.models.chain; contract-tested)
    │                      # Includes ChainCapture.sensitive: bool
    ├── status.py          # UploadRow, TERMINAL_STATES (poll_until stop-set),
    │                      # SortKey, TokenSlot, StatsResponse
    ├── admin.py           # filter and response models, plus ChainAdminDetail
    │                      # (admin-only; outside the contract test)
    │                      # Credential-push bodies: SigningService,
    │                      # SigV4StaticCredBody, ProfileRefCredBody,
    │                      # CredentialPushBody (mirror the server per ADR-012)
    └── envelope.py        # ResponseHeaders parser
```

## Public surface

`phantom_client.__init__` re-exports the full public surface (79
names): `PhantomClient`; the config types (`ClientConfig`,
`Timeouts`, `RetryPolicy`, `SubmitOptions`); every ADR-010 model;
the status and admin models (`TERMINAL_STATES`, `UploadRow`,
`ChainAdminDetail`, the response models, the admin filters); every
error class plus the `EXCEPTION_FOR_CODE` map; the credential-push
bodies (`SigningService`, `SigV4StaticCredBody`,
`ProfileRefCredBody`, `CredentialPushBody`); the `X-Phantom-*`
header constants with `build_request_headers` /
`parse_response_headers`; and the pollers (`poll_until`,
`poll_group_until_finished`). `Transport` is internal-only.

`submit_chain` is the only chain-submission method;
`push_credential` provisions a destination SigV4 credential (below).

## Behaviors worth knowing

- **`submit_chain`** is the only chain-submission method. JSON vs.
  multipart encoding is selected by the presence of `body_refs`.
  Multipart parts are named `envelope` and `body_refs[<name>]` per
  ADR-010, and every part carries a non-empty filename so binary
  bodies round-trip byte-identically.
- **Retries are transport-class only.** The SDK retries on
  `httpx.ConnectError`, `httpx.ReadTimeout`, `httpx.WriteTimeout`,
  and `httpx.PoolTimeout` up to `RetryPolicy.max_attempts`. It
  **never retries 4xx/5xx**. Phantom IS the retry engine. Every
  attempt carries the same `X-Phantom-Idempotency-Key` (defaults
  to `str(envelope.chain_id)`) so Phantom dedupes if it actually
  received the earlier attempt.
- **`TERMINAL_STATES`** is the default stop-set for `poll_until` and
  covers every terminal `ChainState`: `succeeded`, `failed`, `stored`,
  `cancelled`, `expired`, `corrupted` (reached on body-verification
  failure; Phantom never retries it), and `auth_expired`.
  To poll *through* `auth_expired`, pass
  `terminal_states=frozenset({"succeeded", "failed"})`.
- **No bearer values in admin responses.** `TokenSlot` carries
  `endpoint`, `uid`, `last_updated`, `status` only.
- **Destination SigV4 credentials.** `push_credential(dest_host=...,
  credential=...)` provisions a host-keyed SigV4 credential by issuing
  `PUT /v1/admin/credentials/{dest_host}`, the SigV4 analogue of
  `push_token`. The secret is never returned (the server replies `204`)
  and there is no list-credentials read (no server endpoint). Construct
  the body (`SigV4StaticCredBody` or `ProfileRefCredBody`) with a
  `SigningService` **member** (e.g. `service=SigningService.S3`), not a
  raw string: the client model is strict and has no coercer.
- **One base URL.** Intake, admin, and health all ride `phantom_url`;
  Phantom serves them on a single listener (loopback by default per
  ADR-004), so there is no separate admin URL to configure.
- **Async-first.** No sync facade; wrap with `asyncio.run`.

## Error model

Imported from `phantom_client.errors`:

- **Transport-class** (retry-eligible by the SDK):
  `PhantomTransportError` (base), `PhantomConnectError`,
  `PhantomTimeoutError`, `PhantomNetworkError`.
- **HTTP-class** (NOT retried by the SDK; Phantom IS the retry
  engine): `PhantomHttpError` (base) and per-status subclasses
  `PhantomBadRequestError` (400), `PhantomUnauthorizedError` (401),
  `PhantomNotFoundError` (404), `PhantomConflictError` (409),
  `PhantomPayloadTooLargeError` (413), `PhantomUnprocessableError`
  (422), `PhantomValidationError` (422: envelope_invalid /
  body_ref_*), `PhantomRateLimitedError` (429),
  `PhantomServerError` (5xx), `PhantomUnavailableError` (503).
- **SDK-side validation errors**: `PhantomEnvelopeError`
  (server returned malformed `ErrorEnvelope`), `PollDeadlineExceeded`,
  `EmptyFilterError`.

See the error-code matrix ADR (under `docs/adr/017-*` in the
upstream repository) for the authoritative `error.code` →
`(HTTP status, exception class, condition)` table.

## Tests

```bash
# From the workspace root:
uv run pytest src/phantom-client/tests
```

Runs in well under a second against `httpx.MockTransport`.
`mypy --strict` clean. `ruff check` + `ruff format --check` clean.
Contract tests at the workspace root assert byte-equality with
`phantom.models.chain`.
