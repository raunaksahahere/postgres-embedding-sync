-- Several named pipelines (tables) in one sink. Every document is identified
-- by (pipeline, source_key); rows written before this migration belong to the
-- pipeline "default", which is what the single-table configuration maps to,
-- so an upgraded installation keeps serving its documents unchanged.
SET ROLE {{.SinkRole}};

ALTER TABLE {{.Schema}}.source_state ADD COLUMN IF NOT EXISTS pipeline text NOT NULL DEFAULT 'default';
ALTER TABLE {{.Schema}}.source_state DROP CONSTRAINT IF EXISTS source_state_pkey;
ALTER TABLE {{.Schema}}.source_state ADD CONSTRAINT source_state_pkey PRIMARY KEY (pipeline, source_key);

ALTER TABLE {{.Schema}}.dlq ADD COLUMN IF NOT EXISTS pipeline text NOT NULL DEFAULT 'default';
DROP INDEX IF EXISTS {{.Schema}}.dlq_one_open_per_key;
CREATE UNIQUE INDEX dlq_one_open_per_key ON {{.Schema}}.dlq (generation_id, pipeline, source_key) WHERE state = 'open';

ALTER TABLE {{.Schema}}.backfill_jobs ADD COLUMN IF NOT EXISTS pipeline text NOT NULL DEFAULT 'default';
DROP INDEX IF EXISTS {{.Schema}}.backfill_one_live_per_generation;
CREATE UNIQUE INDEX backfill_one_live_per_generation
    ON {{.Schema}}.backfill_jobs (generation_id, pipeline) WHERE state IN ('pending', 'running');

-- Per-pipeline transform fingerprints of each generation. Existing
-- generations were built for the single table, i.e. the "default" pipeline.
ALTER TABLE {{.Schema}}.generations ADD COLUMN IF NOT EXISTS pipelines jsonb NOT NULL DEFAULT '{}';
UPDATE {{.Schema}}.generations
   SET pipelines = jsonb_build_object('default', jsonb_build_object('transform_fingerprint', transform_fingerprint))
 WHERE pipelines = '{}';

-- Chunk tables of existing generations: add the pipeline to the identity.
DO $$
DECLARE
    r    record;
    pk   text;
BEGIN
    FOR r IN SELECT chunk_table FROM {{.Schema}}.generations LOOP
        IF to_regclass(format('{{.Schema}}.%I', r.chunk_table)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format('ALTER TABLE {{.Schema}}.%I ADD COLUMN IF NOT EXISTS pipeline text NOT NULL DEFAULT %L', r.chunk_table, 'default');
        SELECT conname INTO pk FROM pg_constraint
         WHERE conrelid = format('{{.Schema}}.%I', r.chunk_table)::regclass AND contype = 'p';
        IF pk IS NOT NULL THEN
            EXECUTE format('ALTER TABLE {{.Schema}}.%I DROP CONSTRAINT %I', r.chunk_table, pk);
        END IF;
        EXECUTE format('ALTER TABLE {{.Schema}}.%I ADD CONSTRAINT %I PRIMARY KEY (pipeline, source_key, chunk_no)',
                       r.chunk_table, r.chunk_table || '_pkey');
        EXECUTE format('ALTER TABLE {{.Schema}}.%I ALTER COLUMN pipeline DROP DEFAULT', r.chunk_table);
    END LOOP;
END
$$;

-- The default only labelled existing rows; every writer names its pipeline.
ALTER TABLE {{.Schema}}.source_state ALTER COLUMN pipeline DROP DEFAULT;
ALTER TABLE {{.Schema}}.dlq ALTER COLUMN pipeline DROP DEFAULT;
ALTER TABLE {{.Schema}}.backfill_jobs ALTER COLUMN pipeline DROP DEFAULT;

-- The accepted relation shape is now per pipeline. The checkpoint keeps the
-- stream-wide state (LSN, slot identity, schema pause).
CREATE TABLE IF NOT EXISTS {{.Schema}}.pipeline_relations (
    pipeline           text PRIMARY KEY,
    source_table       text NOT NULL,
    key_columns        text[] NOT NULL,
    key_kinds          text NOT NULL,
    relation_signature text NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
INSERT INTO {{.Schema}}.pipeline_relations (pipeline, source_table, key_columns, key_kinds, relation_signature)
SELECT 'default', source_table, ARRAY[split_part(split_part(relation_signature, ',', 1), ':', 1)], key_kind, relation_signature
  FROM {{.Schema}}.checkpoints WHERE consumer = 'main'
ON CONFLICT (pipeline) DO NOTHING;

-- Which pipeline the stream paused on (NULL with a pause from before this
-- migration means the single table, "default").
ALTER TABLE {{.Schema}}.checkpoints ADD COLUMN IF NOT EXISTS schema_pause_pipeline text;
-- No longer written: a binary from before this migration refuses to start on
-- the NULLs instead of streaming with a single-table view of a multi-table sink.
ALTER TABLE {{.Schema}}.checkpoints ALTER COLUMN source_table DROP NOT NULL;
ALTER TABLE {{.Schema}}.checkpoints ALTER COLUMN key_kind DROP NOT NULL;
ALTER TABLE {{.Schema}}.checkpoints ALTER COLUMN relation_signature DROP NOT NULL;

RESET ROLE;
