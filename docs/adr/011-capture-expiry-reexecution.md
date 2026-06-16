# 011. Capture-expiry re-execution: per-instance YAML knob, default off

ADR-009 declares that when a captured value's `ttl_seconds` elapses before later steps in a chain complete, Phantom re-executes the producing step to refresh the capture. This re-execution is safe only when the upstream honors the per-step `idempotency_header` declared in the chain envelope (ADR-010) — otherwise the second execution of step 1 creates a duplicate record at the upstream. Whether a given upstream honors idempotency is a per-deployment fact verified out-of-band (API docs, vendor confirmation, or a one-off staging test); Phantom cannot dynamically detect it without itself creating duplicates as a side effect. So capture-expiry re-execution is controlled by a per-instance YAML knob `capture_reexecution: bool` (default `false`). With `false`, Phantom does NOT re-execute when a captured value expires; the chain transitions to `stored` (body retained per the retention model) and the operator recovers via `GET /v1/admin/export.tar` plus manual re-submission through some other channel. With `true` (set by the operator after verifying upstream idempotency support), Phantom enables ADR-009's full re-execution behavior — captured-value TTL stops being a hard ceiling on buffering. Flipping the knob requires no code change. It defaults `false` pending verification that the upstream honors `Idempotency-Key` on `POST /v2/files`; the operator query is parallel work and can be answered while the rest of the system is implemented.

```yaml
# Excerpt from phantom.yaml — per-instance
instances:
  - id: prod
    target_url_prefix: "https://*.upstream.example"
    refresh:
      strategy: ad_client_credentials
      # ...
    capture_reexecution: false   # ship default; flip true after verifying the upstream honors Idempotency-Key
```

Status: Accepted
Date: 2026-05-12
