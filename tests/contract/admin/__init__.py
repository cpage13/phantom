"""Admin route contract tests (plan § 6.2.7).

One test module per admin endpoint family:

- :mod:`test_status_endpoints` — /health, /ready, /stats, /status,
  /instances, /instances/{id}/status, /reload.
- :mod:`test_chains_endpoints` — /chains list + get + error paths.
- :mod:`test_tokens_endpoints` — /tokens list + push + delete.

Existing modules in ``tests/contract/`` already cover
observability + quarantine + admin model alignment; the modules
here focus on the previously-uncovered admin surface.

Each test boots a minimal admin-only FastAPI app via the shared
:func:`admin_app` fixture and exercises the wire shape with a
:class:`fastapi.testclient.TestClient`.
"""
