# 012. Duplicated chain and admin schemas across phantom and phantom-client

`phantom.models.chain` and `phantom_client.models.chain` are byte-duplicated copies of the ADR-010 envelope shapes. Same fact for the subset of `phantom.models.admin` shapes that the SDK consumes (e.g., `ChainAdminDetail`). This ADR documents the existing decision — it is not introducing a new one — and names the enforcement contract.

The decision: keep the duplication. Enforce wire identity via contract tests at `tests/contract/test_chain_models_alignment.py` and the sibling admin-alignment tests, which import both modules and assert field-by-field equality (name, type, default, description, validators) on every model in the shared set.

Rationale: `phantom-client` ships standalone with no corporate-internal dependency tree. Its runtime dependencies are exactly `httpx` and `pydantic`. Importing `phantom` directly would pull in `aiosqlite`, `psutil`, every emulator dependency the test fixtures touch, and the full server-side composition graph — none of which an SDK consumer should have to install. A shared third package (`phantom-models`) was rejected because the cost (a third workspace member, a third release cadence, a third pyproject) exceeds the cost the contract test makes already-bounded.

Trade-off the operator accepts: every model edit is a two-file edit. The contract test catches drift immediately — it runs in every PR's CI before E2E — so the cost is "annoying but mechanical," not "subtle correctness risk."

`ChainAdminDetail` is admin-only and intentionally outside the contract test. The wire-facing `ChainResponse` (returned by `POST /v1/send`) is byte-mirrored and contract-tested; the admin-facing `ChainAdminDetail` (returned by `GET /v1/admin/chains/{chain_id}`) ships in both packages but is not held to byte-equality. The SDK is free to extend `ChainAdminDetail` with admin-only fields (e.g., `tier`, `committed`, `attempts`, `last_error`) without coupling to the wire schema.

Status: Accepted
Date: 2026-05-14
