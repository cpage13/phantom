-- Phantom SQLite schema for uploads.db (one per instance).
-- The pre-Phase-1 two-tier split (separate :memory: and disk DBs with
-- identical schemas) collapsed into ONE persistent uploads.db holding
-- the ``uploads`` + ``idempotency_index`` tables. The ``body_location``
-- column replaces the old ``committed`` + ``tier`` columns as the source
-- of truth for which BodyStore is holding the body files. Live tokens
-- are NOT here: the token cache is a deliberate second database file
-- (token_cache.db, its own connection and writer; see
-- storage/token_cache.py). The token_cache table declared below is
-- uploads.db's deliberately-kept empty copy of that DDL.

CREATE TABLE IF NOT EXISTS uploads (
    chain_id                         TEXT PRIMARY KEY,           -- UUID, hex-stringified
    instance_id                      TEXT NOT NULL,
    -- The query-grouping axis. No DDL default: admission always supplies
    -- a value (the X-Phantom-Group-Id header value, else chain_id), so
    -- every upload is a group of one by default.
    group_id                         TEXT NOT NULL,
    -- Multi-file association id. NULL means standalone (not part of a
    -- multi-file set); SQL NULL never equals NULL, so standalone rows
    -- can never correlate accidentally.
    multifile_id                     TEXT,
    -- Recorded position within a multi-file set. Display only; never
    -- enforced at delivery.
    send_order                       INTEGER NOT NULL DEFAULT 0,
    route_name                       TEXT NOT NULL,
    state                            TEXT NOT NULL,              -- ChainState
    -- 'ram' | 'file'; flipped only by the PersistController after fsync
    -- (commit-last-column ordering — single-writer manifest invariant
    -- #6, plan § 0.5). Source of truth for which BodyStore is holding
    -- the body files; the corresponding `idx_uploads_body_location`
    -- index supports the admin filter + invariant-audit row walk.
    body_location                    TEXT NOT NULL CHECK (body_location IN ('ram', 'file')),
    attempts                         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at                  TEXT,                       -- ISO-8601 UTC
    received_at                      TEXT NOT NULL,
    -- ISO-8601 UTC. Stamped once on confirmed delivery, never moved;
    -- survives replay. NULL until the upload is delivered.
    sent_at                          TEXT,
    updated_at                       TEXT NOT NULL,
    last_error                       TEXT,
    endpoint                         TEXT NOT NULL,
    uid                              TEXT NOT NULL,              -- credential cache axis (X-Phantom-Uid)
    chain_envelope_json              TEXT NOT NULL,
    captured_values_json             TEXT NOT NULL DEFAULT '{"steps": {}}',
    current_step_index               INTEGER NOT NULL DEFAULT 0,
    idempotency_key                  TEXT NOT NULL,
    capture_reexecution_active       INTEGER NOT NULL DEFAULT 0,  -- bool
    storage_encoding                 TEXT NOT NULL DEFAULT 'original',
    body_size_bytes                  INTEGER NOT NULL DEFAULT 0,
    body_discarded_at                TEXT,
    upstream_status_code             INTEGER,
    upstream_response_headers_json   TEXT,
    last_step_completed              TEXT,
    body_hashes_json                 TEXT NOT NULL DEFAULT '{}', -- {name: {body_hash, storage_hash}}
    -- The producer-supplied X-Phantom-Idempotency-Key header captured at
    -- admission. Stored on the row so admission's dedup probe can
    -- find a live chain by the same ingress key even if the
    -- idempotency_index entry was reaped. NULL when the producer did not
    -- send the header.
    chain_id_at_ingress              TEXT
);

CREATE INDEX IF NOT EXISTS idx_uploads_chain_id_at_ingress
    ON uploads(chain_id_at_ingress);

CREATE INDEX IF NOT EXISTS idx_uploads_state_next_attempt
    ON uploads(state, next_attempt_at);

-- Full index: group_id is NOT NULL, so there are no rows to exclude.
CREATE INDEX IF NOT EXISTS idx_uploads_group_id
    ON uploads(group_id);

CREATE INDEX IF NOT EXISTS idx_uploads_multifile
    ON uploads(multifile_id, send_order);

CREATE INDEX IF NOT EXISTS idx_uploads_instance
    ON uploads(instance_id);

CREATE INDEX IF NOT EXISTS idx_uploads_updated_at
    ON uploads(updated_at);

CREATE INDEX IF NOT EXISTS idx_uploads_body_location
    ON uploads(body_location);

-- Deliberate duplicate of SqliteTokenCache's DDL (storage/token_cache.py).
-- Production tokens live in the separate token_cache.db; this copy sits
-- empty in uploads.db. A change to either definition must land in both.
CREATE TABLE IF NOT EXISTS token_cache (
    endpoint        TEXT NOT NULL,
    uid             TEXT NOT NULL,
    bearer          TEXT NOT NULL,
    observed_at     TEXT NOT NULL,
    source          TEXT NOT NULL,
    status          TEXT NOT NULL,
    PRIMARY KEY (endpoint, uid)
);

-- X-Phantom-Idempotency-Key dedup table.
CREATE TABLE IF NOT EXISTS idempotency_index (
    chain_id_at_ingress  TEXT PRIMARY KEY,
    chain_id             TEXT NOT NULL
);
