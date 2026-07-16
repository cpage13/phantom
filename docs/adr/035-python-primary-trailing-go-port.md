# 035. Python stays primary; a Go port of the service trails it

Status: Accepted
Date: 2026-07-16

## Context

Phantom's service is Python (FastAPI + asyncio on the composition-root
TaskGroup design). The deployment target is producer-side boxes where the
container image footprint matters; the owner wants a substantially smaller,
lighter image at some point. The measured Python image floor is roughly
110 to 120 MB uncompressed (about 45 to 55 MB compressed); a static Go
binary on a distroless base lands around 15 to 30 MB with resident memory
in the tens of megabytes. No performance bottleneck has been observed; the
motive is footprint, not speed. The service is I/O-bound, and its hardest
properties (crash-safe write ordering, supervision, protocol fidelity) live
in the design and the test suite rather than in any language's type system.

Two prerequisites already exist in the repository. First, the e2e suite
validates the service as a black box from input to output: the
`conformance`-marked modules assert only over the config file, HTTP/UDS,
on-disk artifacts, and the emulator, and the subprocess harness launches
whatever binary `E2E_SERVICE_CMD` names. Second, `contracts/` carries the
language-neutral wire, admin, and config contracts, generated from the
Python models and drift-gated in CI.

Rust was considered for the port. It wins on resident memory and on native
discriminated unions, but costs async-cancellation complexity, slower
iteration, and a bet on the younger Azure identity SDK. Go's errgroup model
maps one-to-one onto the composition-root supervision design, and its
first-party `azidentity` and `aws-sdk-go-v2` signers cover the two auth
arms Phantom depends on.

## Decision

Python is the primary and reference implementation, permanently. Every
change lands in Python first, is hardened there, and only then is the
corresponding change applied to the port. The port is Go, covers
phantom-service only, and is always a deliberate follower: it is never
co-equal, and it never accepts changes the Python implementation does not
already have.

The Python e2e suite is the shared executable specification. The port's
acceptance gate is `E2E_SERVICE_CMD=<go-binary> pytest -m conformance`,
plus byte-level fidelity to the `contracts/` artifacts. The emulator, the
Python client SDK, and the e2e suite itself stay Python permanently; a Go
client is out of scope unless a concrete consumer needs one.

## Consequences

- The dual-implementation world is operable: `GET /v1/admin/status` reports
  `implementation` and `service_version`, so an operator can always tell
  which binary answered.
- The conformance marker's classification rule is load-bearing: a test that
  pins Python-internal behavior must stay unmarked, or the gate lies to the
  port. In-process-stack tests are Python-implementation tests by
  construction; the port needs its own equivalents for its composition
  internals, plus its own unit suite and static analysis.
- Dual maintenance is a real ongoing cost, accepted for the footprint win.
  The Python-first discipline is what keeps the suite a specification
  rather than an arbiter of disputes between two drifting implementations.
- Deferred to port start, deliberately: freezing the open protocol
  semantics (the ADR-011 capture-reexecution verification, the per-step
  response-size cap), a data-dir compatibility decision (whether the Go
  binary must boot a Python-written data dir or only fresh deployments),
  and a baseline tag so the port targets a fixed commit.
