# 011. Capture-expiry re-execution: per-instance YAML knob, default off

ADR-009 declares that when a captured value's `ttl_seconds` elapses before later steps in a chain complete, Phantom re-executes the producing step and records a new observation of its captures. A declared per-step `idempotency_header` (ADR-010) can prevent the second execution from creating a duplicate record only when the upstream honors that contract. It does not by itself renew a capability returned in the response. Whether an upstream honors identity deduplication and whether its replayed capability remains usable are per-deployment facts verified out-of-band (API docs, vendor confirmation, or a staging test); Phantom cannot safely infer either fact dynamically. Capture reexecution is therefore controlled by a per-instance YAML knob `capture_reexecution: bool` (default `false`). With `false`, Phantom does not reexecute when its capture observation expires; the chain transitions to `stored` (body retained per the retention model) and the operator recovers via `GET /v1/admin/export.tar` plus manual re-submission through another channel. With `true`, Phantom enables ADR-009's reexecution path under the two-part operator contract stated below. Flipping the knob requires no code change.

## Amendment — 2026-07-12: observation lifetime is not capability lifetime

`ChainCapture.ttl_seconds` drives Phantom's local **capture-observation TTL**:
it controls when Phantom stops trusting the recorded value and either stores or
rewinds the chain. It is not evidence of the actual upstream capability's
lifetime. Reexecuting the producing step records a fresh Phantom observation.
If the upstream returns an exact cached response, that replay can preserve the
logical object identity while returning the same URL/token; it cannot revive a
capability that has actually expired at the upstream.

An operator may set `capture_reexecution: true` only after verifying both:

- **Identity/idempotency semantics:** the declared header prevents duplicate
  logical object creation for this operation.
- **Capability semantics/lifetime:** replay renews the capability, returns a
  replacement capability tied to the same identity, or returns an exact cached
  capability whose real lifetime safely exceeds Phantom's complete
  observation, retry, and buffering window.

Idempotency support alone is insufficient. If the upstream returns an exact
cached response containing a URL that can expire before Phantom finishes, the
operator must leave reexecution disabled (or choose a conservative observation
TTL/window that is provably inside the upstream lifetime). Actual capability
expiry needs its own upstream recovery contract; advancing Phantom's local
timestamp cannot repair it.

The strict executable matrix is `tests/e2e/test_e2e_06_capture_expiry.py`.
It pins the emulator's capability and idempotency windows beyond the whole
scenario budget plus a named margin while Phantom's observation TTL is
deliberately short. The keyed case
proves that two successful metadata responses carry the same declared key and
resolve to one logical identity, token, and URL (the second is an emulator cache
hit), followed by exactly one successful PUT. The unkeyed case proves the risk:
two metadata responses contain distinct identities, tokens, and URLs, and the PUT
uses the second result. The default-disabled control omits the knob and proves
one successful metadata response, no accepted PUT, and terminal `stored`. A
separate upload-scoped `error_rate_5xx` oracle proves at least one PUT was
rejected by that branch before rewind; it intentionally excludes
`unavailable_until` and global-pause 503s. The successful-event oracle does not
count rejected attempts. These are strict tests, not expected failures.

```yaml
# Excerpt from phantom.yaml — per-instance
instances:
  - id: prod
    target_url_prefix: "https://*.upstream.example"
    refresh:
      strategy: ad_client_credentials
      # ...
    # Ship default. Enable only after verifying both identity deduplication
    # and compatible upstream capability renewal/lifetime.
    capture_reexecution: false
```

Status: Accepted
Date: 2026-05-12
