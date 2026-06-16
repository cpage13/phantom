# 025. Never refuse to boot on a recoverable config

Status: Accepted
Date: 2026-06-07

## Context

One configuration could brick Phantom on boot. The mode-flip guard
`check_body_store_mode` (`runtime/startup_checks.py`) raised
`ConfigInvariantError` (a `ValueError` subclass) when `body_store.mode`
was `all_ram` and the instance's `bodies/` root still held body files
from a prior disk-backed run. The guard existed for a real data-loss
reason (ADR-024 relocated it into the lifespan as a per-instance check):
booting `all_ram` over disk-resident bodies would condemn every
`body_location='file'` row to `corrupted` (RAM-only recovery cannot see
disk bytes) and leak the files (no janitor runs in `all_ram`).

But the cure was a dead server. An operator who flipped the mode without
first clearing or migrating the directory got a process that refused to
start, with no buffered uploads being served and no in-band way to
recover short of an operator shelling in. For a sidecar whose entire
job is to never drop an undelivered upload, refusing to run at all is the
worst possible failure mode: a service that is up but degraded can still
be inspected, drained, and restored; a service that will not boot cannot.

The corruption gate (`run_integrity_gate`, ADR-024) already embodied the
right posture for its case: on a `PRAGMA integrity_check` failure it
moves the unreadable artifacts aside to timestamped siblings, bumps a
counter, and boots fresh (`db_integrity.fail_open=true`). The mode-flip
case was the lone recoverable boot problem still wired to fail-stop
instead of preserve-and-continue.

Two facts make preserve-and-continue safe and simple for the mode-flip
case specifically:

- The backup destinations are siblings in the same directory as the
  sources (the instance `data_root`), so each move is a same-filesystem
  rename and therefore atomic. An individual artifact is wholly at its
  source or wholly at its destination, never torn.
- The corruption mover already relocates a DB + body tree to flat
  timestamped siblings. Reusing it (parameterized by a `reason`) rather
  than writing a second mover keeps one relocation implementation.

There is one asymmetry to handle. The corruption mover needs no
in-progress marker: the corrupt DB's continued presence on disk IS the
re-trigger on the next boot. A mode-switch backup runs over a HEALTHY DB,
which cannot be its own re-trigger. With the body-first move order, a
crash after the body moves but before the DB moves would leave the
`bodies/` root empty, so a marker-less guard on the next boot would see
"safe", boot `all_ram` over a healthy DB whose `file` rows now point at
bodies that live only in the backup, and recovery would condemn exactly
the rows the guard exists to protect.

## Decision

**A recoverable config mismatch backs up and runs; it never refuses to
boot.** Concretely, the `all_ram`-over-populated-disk mode switch now
preserves the existing data in a recoverable backup and starts anyway,
loudly, instead of raising.

1. **`check_body_store_mode` backs up instead of raising.** It returns
   `tuple[Path, Path] | None`: `None` when no backup is needed (not
   `all_ram`, or the `bodies/` root has no chain directories), otherwise
   the destination pair from relocating the live DB + body tree via the
   reason-parameterized mover `quarantine(..., reason="mode_switch")`.
   The function stays a pure decision-plus-move; the WARNING log and the
   `mode_switch_backup_total` counter bump live in the lifespan, driven
   by the non-`None` return.

2. **The one mover is parameterized by a `reason`** (`corrupted` or
   `mode_switch`), not duplicated. Corruption artifacts keep their exact
   names (`uploads.corrupted.<iso>.db`, `bodies.quarantine.<iso>`); a
   mode-switch backup uses one infix for both artifacts
   (`uploads.mode_switch.<iso>.db`, `bodies.mode_switch.<iso>`), with the
   `reason` keyword-only and after `timestamp` so every existing
   positional caller is byte-identical.

3. **The healthy-DB backup gets the re-trigger the corruption path gets
   for free.** A mode-switch backup writes an in-progress marker
   (`BackupMoveMarker`, file `.backup_move.in_progress`, carrying the
   exact destination `iso` and a `direction`) BEFORE moving anything and
   clears it after both moves. A new boot step,
   `reconcile_interrupted_backup_move`, runs after the corruption gate
   and before `check_body_store_mode`: if a marker is present it finishes
   the interrupted move forward idempotently (completing whichever of the
   body / DB / `-wal` / `-shm` artifacts still sits at its source) and
   then clears the marker, so the live tree is fully clean before the
   mode-flip decision runs. The marker is mandatory, not polish: it is
   the only thing that keeps a crash mid-backup from looking "safe" on
   the next boot and condemning the backed-up rows.

4. **Switch-back restore is a one-call admin action, not
   automatic-on-boot.** `POST /v1/admin/quarantine/restore` (with
   `?instance=` when more than one instance is configured) backs up any
   current live data to a fresh `mode_switch` backup first (clobber-safe:
   nothing is overwritten), then moves the chosen backup into the
   now-empty live tree via the same marked mover, so a crash mid-restore
   is finished forward by the same reconciliation. The response always
   sets `restart_required=True`: the route stages the restore on disk; a
   restart in a disk-backed mode (`hybrid` or `all_disk`) is required to
   actually serve the restored data.

### What is deliberately NOT done

- **The corruption fail-closed escape hatch is untouched.** A real
  `PRAGMA integrity_check` failure still honors `db_integrity.fail_open`:
  the default `true` quarantines and boots fresh, and an operator who
  sets `fail_open=false` still gets a deliberate abort after quarantining.
  This ADR changes only the recoverable *config* case, not the corruption
  case.
- **Disk bodies are not auto-migrated into RAM on a mode switch.** The
  evidence rejected silent migration; the decision is to preserve and set
  aside (back up), not to reinterpret the operator's data.
- **The restore route does not hot-reattach the running store.** The
  running store keeps its open file descriptor and will not serve the
  restored data until a restart; a true runtime hot-swap is out of scope
  (it concerns serving-without-restart, not crash-safety, which the
  marked moves already cover).
- **The mode-switch backup is not auto-reaped.** Mode-switch backups are
  operator-managed exactly like corruption quarantines: same inventory,
  same posture, no second reaper.

## Consequences

- **Phantom always boots.** Every database problem at boot is handled
  rather than fatal: the recoverable mode switch backs up and runs, and
  the corruption gate quarantines and runs. The lone recoverable config
  that could brick the service no longer can.
- **No undelivered upload is silently dropped.** The mismatched data is
  preserved in a recoverable backup that appears in the inventory with
  `reason=mode_switch`, and a one-call admin restore moves it back.
- **A crash mid-backup is finished forward, not lost.** Because the
  destinations are same-filesystem sibling renames and a marker records
  the in-progress move, the next boot completes whichever half remains
  before the mode-flip decision runs, then boots clean.
- **One relocation implementation, two reasons.** The mover, the
  inventory, and the reconciliation are shared; the corruption path stays
  byte-identical. There is no parallel mode-switch mover to keep in sync.
- **Restore is explicit and clobber-safe, and requires a restart to
  serve.** The interim backup means a restore never overwrites live data,
  and `restart_required=True` makes the disk-backed-mode restart an
  explicit, documented step rather than a silent expectation.

## Cross-references

- `phantom.runtime.startup_checks.check_body_store_mode` - the guard,
  now back-up-and-run.
- `phantom.storage.integrity` - the reason-parameterized mover
  (`quarantine`), the marked restore mover
  (`restore_mode_switch_backup`), the boot reconciliation
  (`reconcile_interrupted_backup_move`), the `BackupMoveMarker`, and
  the `reason`-aware inventory (`list_quarantines`,
  `QuarantineInventoryEntry`).
- `phantom.app.create_app` - the lifespan that runs the new boot order
  (integrity gate → reconciliation → mode-flip guard) and bumps
  `mode_switch_backup_total`.
- `phantom.routes.admin` - `GET /v1/admin/quarantine` (inventory) and
  `POST /v1/admin/quarantine/restore` (the one-call restore).
- ADR-024 - `app.py`'s lifespan is the composition root that runs the
  per-instance boot guards at the correct scope.
- ADR-014 - the dual-body-hash corruption path the mode-flip guard
  protects (`all_ram` over disk bodies would otherwise condemn `file`
  rows to `corrupted`).
- `CONTEXT.md` "BodyStore" / "Quarantine"; `docs/architecture-intent.md`
  invariant #1 (an undelivered upload is never dropped).
