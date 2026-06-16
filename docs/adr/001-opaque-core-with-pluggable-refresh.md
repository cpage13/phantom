# 001. Opaque Phantom core with pluggable refresh strategies

Phantom-the-service has an opaque HTTP buffer/retry core with one pluggable refresh-strategy slot. Two strategies are shipped: `wait` (default, no-op — upload parks until the cache is refreshed externally) and `ad_client_credentials` (autonomous AD-token minting via Phantom's own app registration). Each deployment selects one strategy at startup via config. This keeps the core upstream-agnostic while supporting autonomous recovery for deployments where it is safe to act under Phantom's own identity (v1 deployments, where uploader identity travels in the request payload as `uploader_id`); v0 deployments use `wait` because uploader identity is bound to the JWT claims and minting under Phantom's SP would corrupt the attribution the upstream records.

Status: Accepted
Date: 2026-05-12
