# 033. Service-based SigV4 re-signing for `aws_sigv4` routes

Status: Accepted
Date: 2026-06-26

## Context

Phantom's opaque core (ADR-001) and its `(endpoint, uid)` bearer cache (ADR-002)
cover the `phantom_bearer` auth mode: Phantom injects a cached `Authorization`
header it never parses. That model does not cover a producer that wants to push
to AWS S3 with a stock S3 SDK (boto3) pointed at Phantom: there the producer
holds no usable upstream credential, and the request that reaches the real
bucket must carry a fresh, valid AWS SigV4 signature. Phantom must therefore be
able to **re-sign** the outbound request itself, on the producer's behalf, for
the `aws_sigv4` route arm (`RouteCfg.auth_mode`, `phantom.config.settings`).

Re-signing AWS SigV4 is not a single algorithm. S3 specifically requires the
`x-amz-content-sha256` payload-hash header, which botocore's base `SigV4Auth`
never emits. Only `S3SigV4Auth` does. A correct signature for S3 is wrong for
a service that does not expect that header, and vice versa. So the signer must
know *which AWS service* it is signing for; that fact cannot be inferred from
the destination URL alone without re-encoding S3-specific knowledge into a
"generic" core (CONTEXT.md: Phantom has zero upstream-specific knowledge).

Three decisions here are hard to reverse, which is why they are recorded:

- whether the service is an **inferred** property or a **declared, required**
  one (it lands in a `StrEnum` that is part of the credential wire schema, which
  the SDK duplicates per ADR-012);
- how the signing credential is **keyed** in its store (a SigV4 forward step has
  no caller-supplied `uid`, so the ADR-002 `(endpoint, uid)` key does not fit);
- the **provisioning + secret-handling** posture (where literals vs env-var
  names live, and what an admin response may echo).

## Decision

For a route whose `auth_mode` is `aws_sigv4`, the chain executor re-signs the
outbound request at egress with a fresh botocore SigV4 signature, dispatched by
a **required `service`** axis, using a credential keyed on the **destination
host alone**.

### Re-signing is dispatched by a required `SigningService`, fail-loud on unknown

`SigningService` (`phantom.models.credential.SigningService`) is a closed
`StrEnum` whose only member today is `S3 = "s3"`: the single source of the
wire string `"s3"`; all logic references `SigningService.S3` by dot notation.
Every destination credential carries an explicit, REQUIRED `service` (the scope
sibling of `region`), declared at provision time and never defaulted or
inferred. The signer dispatches on it through a single table
(`_SERVICE_SIGNERS: dict[SigningService, type[SigV4Auth]]` in
`phantom.chain.sigv4_signer`) to select the botocore signer class; a `service`
with no dispatch entry raises `SigV4SigningError` (a parkable error the executor
catches, not a bare `KeyError`). Declaring the service rather than inferring it
makes an unknown or missing service fail loud at the pydantic boundary (`422`
admin / `ValidationError` config) rather than being silently mis-signed.

### S3 maps to `S3SigV4Auth` (the `x-amz-content-sha256` requirement)

`SigningService.S3` maps to botocore's `S3SigV4Auth`, NOT base `SigV4Auth`,
precisely because S3 requires `x-amz-content-sha256` and base `SigV4Auth` never
emits it. `sign_sigv4(*, method, url, headers, body, credential)` mutates the
caller's `headers` in place; the botocore signer's `add_auth(request)`
populates `Authorization`, `X-Amz-Date`, `x-amz-content-sha256`, and (when an
STS session token is present) `X-Amz-Security-Token`. The caller's mapping is
then REBUILT from botocore's signed view rather than merged key by key: that
view is case-insensitive and the caller's dict is not, so a merge left the
caller's lowercase original beside every name botocore rewrote, and a
client-signed request arrives lower-cased (starlette lower-cases inbound
names). Two `Authorization` lines on the wire earn a 403
SignatureDoesNotMatch. Rebuilding makes a duplicate structurally impossible
for every header botocore rewrites, known or future; nothing is lost, because
the view was seeded from the caller's own mapping and `add_auth` only adds and
replaces. Adding a future service is a new `SigningService`
member plus a `_SERVICE_SIGNERS` entry, not a redesign.

### The credential store keys on the destination host alone (`HostCredKey`)

The signing credential lives in the optional per-instance `signer_creds` store,
keyed by `HostCredKey` = the lower-cased resolved destination host alone (no
`uid`). This is the deliberate, forced difference from the ADR-002 token cache:
a SigV4 forward step is synthesized by Phantom (it has no caller-supplied
credential id), so the `uid` axis has no value to carry and is dropped. ADR-002
is untouched: `uid` stays inert under `aws_sigv4`. The executor looks the
credential up by `HostCredKey(_hostname(full_url))`; a missing or failed
credential marks the slot bad and parks the row `auth_expired` (ADR-032), which
the sigv4-flavoured `Kicker` (the SigV4 analogue of the bearer flavour) wakes
on a fresh push. The store is persistent at rest, surviving restart (mirrors ADR-003).

### Two provisioning routes; the secret is never echoed

A credential reaches the store two ways, both carrying a required `service`:

- **Runtime admin push**: `PUT /v1/admin/credentials/{dest_host}`
  (`phantom.routes.admin`), body the discriminated `CredentialPushBody`
  (`sigv4_static` resolved literals, or a `profile_ref`). It replies `204 No
  Content` with no body, so the `secret_access_key` is never echoed; the
  `{dest_host}` segment is normalized through the SAME `_hostname` helper the
  executor uses, so push-key == lookup-key by construction.
- **Boot-time config**: the top-level `sigv4_credentials: list[SigV4CredentialCfg]`,
  which holds env-var NAMES only (`access_key_id_env`, `secret_access_key_env`,
  `session_token_env`), never the secret literal; the names are resolved to
  literals at boot and materialized into every instance's store under
  `source="config"`.

The admin surface exposes credential STATUS only, never secret material, and
there is no GET/LIST credential endpoint today (relates to ADR-004: admin is
loopback, no auth; and ADR-003: persistent at rest).

### An inbound presigned credential is superseded and stripped

Replacement is the whole point of re-signing, and a presigned request carries
the producer's signature in the QUERY (`X-Amz-Signature`, `X-Amz-Credential`,
`X-Amz-Date`, `X-Amz-Expires`, `X-Amz-Security-Token`, `X-Amz-SignedHeaders`,
`X-Amz-Algorithm`) rather than in a header. On an `aws_sigv4` route the
executor removes that whole parameter set from the step URL before signing, so
Phantom's fresh signature is the only credential on the wire. Forwarding both
would present two authentication mechanisms and earn a 4xx, and orphaned
`X-Amz-Credential` / `X-Amz-Date` parameters would put the client's credential
identifiers inside Phantom's own canonical query string. Every other query
parameter survives byte-for-byte. This is the query-carrier analogue of
rebuilding the outbound header map from the signed set.

The strip is ROUTE-scoped, not carrier-scoped: it applies to any step Phantom
sends on an `aws_sigv4` route, including a producer-authored chain. The other
two auth modes keep the presigned set, because there Phantom replaces no
signature: `none` IS the forward-as-is presigned case, and a `phantom_bearer`
route pairs two unrelated credential systems rather than two signatures. An
operator who wants an inbound presigned credential honoured declares the route
`auth_mode: none`.

### Re-signing preserves the body bytes

Re-signing replaces only the auth headers; the body bytes Phantom forwards are
byte-identical to what the producer sent. The transparent-on-the-wire guarantee
and the dual-body-hash discipline (ADR-014) are unchanged: `x-amz-content-sha256`
is the SHA-256 of those same unchanged bytes.

## Consequences

- **A stock S3 SDK needs no Phantom-specific code.** A producer points boto3's
  `endpoint_url` at Phantom over plain HTTP with placeholder credentials;
  Phantom re-signs each object write for the real bucket with the host-keyed
  credential an operator provisioned.
- **An unknown / missing service fails loud, never silently mis-signs.** The
  required `service` is rejected at the pydantic boundary; an unmapped service
  raises a parkable `SigV4SigningError`.
- **A bearer-only deployment is unaffected.** The `signer_creds` store is
  optional; instances without it skip credential pushes (no-op, not a crash),
  and `aws_sigv4` is opt-in per route.
- **Adding a second AWS service is additive.** A new `SigningService` member +
  a `_SERVICE_SIGNERS` entry, with no change to the store key, the provisioning
  routes, or the executor branch.
- **The credential schema is a duplicated wire contract (ADR-012).** The SDK
  mirrors `SigningService` + the credential bodies; the two copies must move
  together.

## Cross-references

- `phantom.models.credential`: `SigningService`, the `CredentialPushBody`
  discriminated union, `HostCredKey`, and the internal `DestinationCredential`.
- `phantom.chain.sigv4_signer`: `sign_sigv4`, the `_SERVICE_SIGNERS` dispatch
  table, and `SigV4SigningError`.
- `phantom.config.settings`: `RouteCfg.auth_mode` (the 3-valued selector),
  `SigV4CredentialCfg` (the boot-time config arm), and `phantom_default_target`.
- `phantom.routes.admin`: `PUT /v1/admin/credentials/{dest_host}` (the runtime
  push); `phantom.workers.kicker.Kicker` in its `AWS_SIGV4_FLAVOUR` (the
  parked-row waker).
- ADR-001: opaque core with pluggable refresh strategies (the model
  `aws_sigv4` extends).
- ADR-002: token cache keyed by `(endpoint, uid)`; this ADR's host-alone key
  is the forced contrast.
- ADR-003: persistent token cache; the credential store mirrors its at-rest
  persistence.
- ADR-004: admin API on loopback with no auth; the secret-never-echoed posture
  rides on it.
- ADR-012: duplicated chain/admin schemas; the credential schema the SDK
  mirrors is one more such pair.
- ADR-014: dual body hash; re-signing leaves both hashes intact.
- ADR-032: the `expired` terminal state; an `aws_sigv4` row parks in
  `auth_expired` and is bounded by the send-deadline.
