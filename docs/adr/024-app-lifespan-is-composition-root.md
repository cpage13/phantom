# 024. `app.py`'s lifespan is the sole composition root

Status: Accepted
Date: 2026-05-29

## Context

Phantom had **two** composition surfaces that drifted apart:

- `app.py`'s FastAPI `lifespan` (`phantom.app.create_app`) — the path
  production actually runs (`python -m phantom` → `__main__` →
  `create_app(...)` → `uvicorn.run(app)`). It is multi-instance: N
  instances, each with its own `data_root` / SQLite / `bodies/`, an
  `InstanceDispatcher`, per-instance workers, FastAPI dependency
  overrides, and SIGHUP / hot-reload.
- `runtime/composition.py::compose_and_run` (+ a `Runtime` dataclass) —
  a single-instance shim created in Phase 1 (Slice 1.C) as the *intended*
  future root. The lifespan-collapse migration (Slice 1.E) that would
  have made `lifespan` delegate to it **never landed**. `compose_and_run`
  had **zero** production callers — every `async with compose_and_run(...)`
  was under `tests/`.

The drift was not cosmetic. Five startup behaviors lived **only** in the
dead `compose_and_run` path and were therefore absent from production
despite being documented and "tested":

1. the DB integrity-check / quarantine gate,
2. the `all_ram`-over-populated-disk fail-closed guard,
3. the retention-floor config invariant,
4. `os.umask(0o077)` bare-metal file-perm hardening,
5. the optional `ColdBackupScheduler`.

Their tests drove `compose_and_run` exclusively, so the suite was green
while production ran unguarded. The two roots had even diverged on the
on-disk body path — `compose_and_run` rooted bodies at
`data_root/"body_store"` while production uses `data_root/"bodies"` —
concrete proof that two hand-maintained roots cannot stay honest.

A separate latent risk: there was no validation that configured
instances were mutually isolated. A duplicate instance `id` silently
collapses in the dispatcher's `_by_id` map (an instance becomes
unreachable); a shared `data_dir` puts two stores on one `uploads.db`
(a single-writer violation that corrupts the DB).

## Decision

**`app.py`'s lifespan is the one and only composition root.** The
abandoned migration is reversed in direction: rather than rebuild the
working multi-instance lifespan into a transport-agnostic
`compose_and_run`, the dead shim is removed and the lifespan is made the
documented root it already was in practice.

1. **`compose_and_run` and `Runtime` are deleted** (no backwards-compat
   shim — nothing in production used them). The relocatable guard logic
   moved to a typed seam, `runtime/startup_checks.py`, which both the
   lifespan and the re-pointed tests call so there is exactly one
   implementation. `runtime/composition.py` is gone; tests now drive the
   real path (`create_app` + the e2e `boot_stack` harness).

2. **The five startup behaviors run in the lifespan, at the correct
   scope:**
   - **Process-wide, once at the top** (settings + perms are process
     global): `apply_umask()`, `check_retention_floor(settings)`, and
     `check_instance_isolation(settings.instances)`.
   - **Per instance** (after that instance's `data_root.mkdir`, before
     its `SqliteUploadStore` opens and before body-store construction):
     `check_body_store_mode(mode, bodies_root)` and the
     `run_integrity_gate(...)` DB-corruption gate, using **this
     instance's** `data_root` and the production `bodies/` path.
   - **Per instance, into the lifespan `TaskGroup`**: a
     `ColdBackupScheduler` when `db_integrity.backup_enabled` (one per
     instance `uploads.db`, writing to `<data_root>/backups/`).

3. **A new fail-closed instance-isolation invariant**
   (`check_instance_isolation`) rejects, at startup, a duplicate `id`, a
   colliding/`Path.resolve`-nested `data_dir` (true component nesting via
   `Path.parents`, not raw-string prefix — so siblings `a/b` and `a/bc`
   stay distinct), or a duplicate `host_prefix` (lower-cased, exact-match
   only; glob *overlap* stays permitted by declaration order). This
   guarantees each instance is completely isolated: unique identity, its
   own storage partition, and unambiguous routing.

4. **`startup_checks.py` owns the single `build_body_store` mode-wiring
   table** (`hybrid` / `all_ram` / `all_disk`), replacing the two
   drift-prone copies the dual roots carried.

The `IntegrityChecker`, `ConfigInvariantError`, and `ColdBackupScheduler`
are reused, not re-created.

## Consequences

- **Code == docs.** There is one composition root and it is the one that
  runs. `CONTEXT.md`, `docs/architecture-intent.md` (§ 3.6 + invariant
  #15), and `src/phantom-service/README.md` name `app.py`'s lifespan; the
  `runtime/composition.py` references are gone.
- **The guards actually run in production**, at the right scope —
  per-instance corruption/mode guards see the correct `data_root`, and
  process-wide guards run once. The guard tests now exercise the real
  `create_app` path and are genuine falsifiers (removing a guard turns
  its test red).
- **Instances are provably isolated** or the process refuses to start
  with an operator-actionable `ConfigInvariantError`.
- **The spawn-site invariant became enforceable.** With one root, the
  `forbid-asyncio-create-task-outside-composition` pre-commit hook was
  rewritten (ADR-cross-ref below) to fail on unsupervised spawns
  (`asyncio.create_task(`, `asyncio.ensure_future(`, `loop.create_task(`)
  outside the lifespan's single sanctioned `loop.create_task` SIGHUP
  site — `tg.create_task(...)` (TaskGroup-supervised) stays allowed, so
  the sender's nested worker-pool TaskGroup remains legitimate.
- **No `runtime/composition.py`.** The `runtime/` package now holds only
  `startup_checks.py` (the boot-guard seam) and a docstring-only
  `__init__.py`.

## Cross-references

- `phantom.app.create_app` — the lifespan composition root.
- `phantom.runtime.startup_checks` — the boot-guard seam:
  `apply_umask`, `check_retention_floor`, `check_instance_isolation`,
  `check_body_store_mode`, `run_integrity_gate`, `build_body_store`,
  `ConfigInvariantError`.
- `scripts/precommit/forbid_create_task.sh` — the rewritten spawn-site
  invariant (architecture-intent.md invariant #15).
- ADR-006 — multi-instance topology (why the root is multi-instance).
- ADR-016 / ADR-020 — the container deployment model the lifespan boots
  under.
- `CONTEXT.md` "Composition root" / "Mode-flip guard" / "Quarantine";
  `docs/architecture-intent.md` § 3.6 + invariant #15.
