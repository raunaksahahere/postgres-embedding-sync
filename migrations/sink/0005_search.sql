-- Filtered and hybrid search. Every chunk row gets the document's declared
-- metadata (meta, GIN-indexed). Adding the column with a constant default
-- rewrites no row. Full-text search uses an expression index on
-- to_tsvector(<search.text_config>, content), which `embedsync run` builds
-- concurrently in the background: no row is rewritten for it either, so
-- the vector index is untouched. Metadata of pipelines and views that
-- declare it is filled by a re-scan that reuses the stored vectors. Until a
-- generation is complete, filtered, text and hybrid requests against it
-- answer 503 search_index_building, while plain vector search keeps working.
SET ROLE {{.SinkRole}};

-- search_target is what the generation's search columns are being built
-- for (metadata declarations + text search configuration);
-- search_fingerprint is what they are complete for.
ALTER TABLE {{.Schema}}.generations ADD COLUMN IF NOT EXISTS search_target text;
ALTER TABLE {{.Schema}}.generations ADD COLUMN IF NOT EXISTS search_fingerprint text;

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT chunk_table FROM {{.Schema}}.generations LOOP
        IF to_regclass(format('{{.Schema}}.%I', r.chunk_table)) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format('ALTER TABLE {{.Schema}}.%I ADD COLUMN IF NOT EXISTS meta jsonb NOT NULL DEFAULT ''{}''', r.chunk_table);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON {{.Schema}}.%I USING gin (meta)', r.chunk_table || '_meta', r.chunk_table);
    END LOOP;
END
$$;

RESET ROLE;
