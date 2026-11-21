-- Document views assembled from related tables. Every table of a view keeps
-- its allowlisted rows in source_state (pipeline = "<view>:<component>"),
-- written in the same transaction as the checkpoint; source_refs indexes the
-- references between them (child -> parent, link -> parent and target), so
-- the stream path finds a parent's rows without querying the source.
-- Additive: nothing existing is rewritten or re-embedded.
SET ROLE {{.SinkRole}};

CREATE TABLE IF NOT EXISTS {{.Schema}}.source_refs (
    pipeline   text NOT NULL,
    source_key text NOT NULL,
    ref        text NOT NULL,
    ref_key    text NOT NULL,
    PRIMARY KEY (pipeline, source_key, ref)
);
CREATE INDEX IF NOT EXISTS source_refs_by_ref ON {{.Schema}}.source_refs (pipeline, ref, ref_key);

-- Backfill job kinds: embed (scan a table into a generation, as before),
-- state (scan a view's table into source_state; no generation) and render
-- (render a view's documents into a generation from state; no scan).
ALTER TABLE {{.Schema}}.backfill_jobs ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'embed'
    CHECK (kind IN ('embed', 'state', 'render'));
ALTER TABLE {{.Schema}}.backfill_jobs ALTER COLUMN kind DROP DEFAULT;
ALTER TABLE {{.Schema}}.backfill_jobs ALTER COLUMN generation_id DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS backfill_one_live_state_job
    ON {{.Schema}}.backfill_jobs (pipeline) WHERE kind = 'state' AND state IN ('pending', 'running');

RESET ROLE;
