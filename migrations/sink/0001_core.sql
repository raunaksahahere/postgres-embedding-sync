-- Sink-side catalog. Owned by the sink role; readers get SELECT on the serving
-- catalog and on each chunk table as it is created.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS {{.Schema}} AUTHORIZATION {{.SinkRole}};
REVOKE ALL ON SCHEMA {{.Schema}} FROM PUBLIC;
GRANT USAGE ON SCHEMA {{.Schema}} TO {{.ReaderRole}};

SET ROLE {{.SinkRole}};

CREATE TABLE IF NOT EXISTS {{.Schema}}.schema_migrations (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

-- One row per replication consumer. lsn is the end LSN of the last source
-- transaction whose effects are durable in this database; it is written in the
-- same transaction as those effects.
CREATE TABLE IF NOT EXISTS {{.Schema}}.checkpoints (
    consumer          text PRIMARY KEY,
    system_identifier text NOT NULL,
    timeline          integer NOT NULL,
    database          text NOT NULL,
    slot_name         text NOT NULL,
    publication       text NOT NULL,
    source_table      text NOT NULL,
    slot_created_lsn  pg_lsn NOT NULL,
    key_kind          text NOT NULL,
    relation_signature text NOT NULL,
    -- checkpoint at which the stream paused on an incompatible relation; a
    -- schema change can only be accepted while the stream is still there
    schema_pause_lsn  pg_lsn,
    lsn               pg_lsn NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{.Schema}}.generations (
    id                    text PRIMARY KEY,
    model_key             text NOT NULL,
    model_name            text NOT NULL,
    model_revision        text NOT NULL,
    dimension             integer NOT NULL,
    distance              text NOT NULL,
    query_prefix          text NOT NULL DEFAULT '',
    document_prefix       text NOT NULL DEFAULT '',
    index_type            text NOT NULL,
    index_params          jsonb NOT NULL,
    transform_fingerprint text NOT NULL,
    chunk_table           text NOT NULL UNIQUE,
    state                 text NOT NULL CHECK (state IN
                            ('pending', 'building', 'ready', 'active', 'previous', 'rejected', 'retired', 'stale')),
    stream_from_lsn       pg_lsn,
    created_at            timestamptz NOT NULL DEFAULT now(),
    ready_at              timestamptz,
    promoted_at           timestamptz,
    retired_at            timestamptz
);

-- The serving alias. Promotion and rollback are single-row updates.
CREATE TABLE IF NOT EXISTS {{.Schema}}.serving (
    name                   text PRIMARY KEY,
    generation_id          text NOT NULL REFERENCES {{.Schema}}.generations (id),
    previous_generation_id text REFERENCES {{.Schema}}.generations (id),
    rollback_until         timestamptz,
    switched_at            timestamptz NOT NULL DEFAULT now()
);

-- Allowlisted column values as of the checkpoint. Used to fill unchanged TOAST
-- values, to verify coverage at a common boundary, and to replay quarantined rows.
CREATE TABLE IF NOT EXISTS {{.Schema}}.source_state (
    source_key text PRIMARY KEY,
    fields     jsonb NOT NULL,
    source_lsn pg_lsn NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{.Schema}}.backfill_jobs (
    id            bigserial PRIMARY KEY,
    generation_id text NOT NULL REFERENCES {{.Schema}}.generations (id),
    state         text NOT NULL CHECK (state IN ('pending', 'running', 'done', 'cancelled')),
    last_key      text,
    rows_scanned  bigint NOT NULL DEFAULT 0,
    windows       bigint NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    completed_at  timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS backfill_one_live_per_generation
    ON {{.Schema}}.backfill_jobs (generation_id) WHERE state IN ('pending', 'running');

-- Versioned quarantine. At most one open record per (generation, key); a newer
-- change to the key supersedes it in the same transaction as that change.
CREATE TABLE IF NOT EXISTS {{.Schema}}.dlq (
    id            bigserial PRIMARY KEY,
    generation_id text NOT NULL,
    source_key    text NOT NULL,
    source_lsn    pg_lsn NOT NULL,
    reason        text NOT NULL,
    error         text NOT NULL,
    attempts      integer NOT NULL DEFAULT 1,
    state         text NOT NULL CHECK (state IN ('open', 'resolved', 'superseded')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    resolved_at   timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS dlq_one_open_per_key
    ON {{.Schema}}.dlq (generation_id, source_key) WHERE state = 'open';

CREATE TABLE IF NOT EXISTS {{.Schema}}.pipeline_status (
    consumer   text PRIMARY KEY,
    state      text NOT NULL,
    reason     text NOT NULL DEFAULT '',
    detail     text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {{.Schema}}.migrations (
    id                bigserial PRIMARY KEY,
    target_generation text NOT NULL REFERENCES {{.Schema}}.generations (id),
    from_generation   text REFERENCES {{.Schema}}.generations (id),
    step              text NOT NULL,
    state             text NOT NULL CHECK (state IN ('running', 'gate_failed', 'promoted', 'rolled_back', 'finalized', 'failed')),
    boundary_lsn      pg_lsn,
    gate_report       jsonb,
    error             text NOT NULL DEFAULT '',
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

GRANT SELECT ON {{.Schema}}.generations, {{.Schema}}.serving TO {{.ReaderRole}};

RESET ROLE;
