# Phantom contracts

This directory holds Phantom's wire, admin, and config contracts as
language-neutral artifacts. Every file here is generated from the Python
source of truth by `scripts/export_contracts.py`. Do not hand-edit any
file in this directory. A hand edit is overwritten the next time the
exporter runs, and CI rejects a working tree where these files do not
match the generator's output.

## Regenerating

Run the exporter from the repository root:

```
uv run python scripts/export_contracts.py
```

To check for drift without writing anything (the same check CI runs):

```
uv run python scripts/export_contracts.py --check
```

`--check` regenerates every artifact into a temporary directory, compares
it byte for byte against this directory, prints a per-file report, and
exits 1 if anything differs.

## Files

- `chain-envelope.schema.json`: JSON Schema for `ChainEnvelope` (ADR-010).
- `chain-response.schema.json`: JSON Schema for `ChainResponse`.
- `error-body.schema.json`: JSON Schema for `ErrorEnvelope`, the
  `{"error": {...}}` shape Phantom returns on every error response.
- `settings.schema.json`: JSON Schema for the top-level `Settings` model
  (the YAML config shape).
- `admin-openapi.json`: the OpenAPI document for the intake, admin, and
  health surface.
- `wire-headers.json`: the `X-Phantom-*` header constant names and values.
- `fixtures/chain-envelope.example.json`: one canonical two-step chain
  envelope.
- `fixtures/error-body.example.json`: one canonical error envelope.

## Why this exists

A planned Go implementation of Phantom needs a contract to build against
that does not require reading Python. These artifacts, plus the e2e
conformance suite, are that contract. When a Python model in
`phantom.models` or `phantom.config.settings` changes, regenerate this
directory in the same change and commit the result.
