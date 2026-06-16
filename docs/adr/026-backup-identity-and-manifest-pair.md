# 026. Backup identity is a uuid; a manifest names the pair

Status: Accepted
Date: 2026-06-10

## Context

A backup (corruption quarantine, mode-switch set-aside, or token-cache
isolate) used to be identified by the human-readable timestamp embedded
in its artifact filenames. Two backups in the same wall-clock second
collided, so a disambiguation probe (`_first_free_quarantine_iso`)
searched for a free suffix, and the DB and body artifacts of one backup
were paired only by filename convention. Stringy time as identity and
convention-paired artifacts were both prior-cycle failure modes: a
same-second restore could address the wrong backup, and a half-present
pair looked restorable when it was not.

## Decision

Every backup's identity is a uuid `backup_id`, minted at creation
(`storage/integrity.py`). The timestamp is demoted to display and sort
material only; artifact names carry the display iso plus the first 8 hex
chars of the `backup_id` for filesystem uniqueness, and no code path
parses a timestamp out of a filename for any load-bearing purpose. The
disambiguation machinery is deleted outright; same-second backups simply
coexist under distinct identities.

Every backup writes ONE manifest (`backup.<backup_id>.manifest.json`, a
small JSON written temp-then-rename into the instance data_root) BEFORE
any artifact moves. The manifest names both artifacts (db path, body
path), which halves exist, the `reason` discriminator, and the display
timestamp. Everything downstream is manifest-driven:

- The inventory (`GET /v1/admin/quarantine`) returns one entry PER
  BACKUP with `has_db` / `has_body` reporting current disk presence. An
  on-disk artifact no manifest claims surfaces as a flagged anomaly
  entry (`backup_id` null) and is never restorable.
- Restore addresses identity:
  `POST /v1/admin/quarantine/restore?backup_id=...&instance=...`. A
  backup whose DB half is missing is refused 409 up front, before any
  live data is displaced.
- The crash-safety marker and `reconcile_interrupted_backup_move`
  (ADR-025) are re-keyed on `backup_id` and complete a half-finished
  move forward using the manifest's DECLARED paths, never filename
  matching.
- The token-cache isolate is manifested like every other backup
  (`body_path` null), so the inventory has one shape.

## Consequences

- Two backups in the same second coexist and both restore correctly;
  the restore handle is exactly the inventory's `backup_id`.
- A stray artifact cannot impersonate a restorable backup: with no
  manifest it has no identity, so the restore route cannot address it.
- A crash between the body move and the DB move is completed forward on
  the next boot by identity, leaving any same-second sibling untouched.
- The manifest is deleted only when the backup is CONSUMED (neither
  declared artifact remains at its backup path), so a real backup never
  decays into an anomaly.

## Cross-references

- `phantom.storage.integrity` - identity mint, manifest write,
  manifest-driven inventory, marker, reconcile.
- `phantom.routes.admin` - inventory and restore routes.
- ADR-025 - the back-up-and-run mover, marker, and one-call restore
  this decision re-keys onto `backup_id`.
