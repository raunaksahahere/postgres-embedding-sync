-- Filtered and hybrid search. Every chunk row gets the document's declared
-- metadata (meta) and a full-text vector of its content (tsv), each with a
-- GIN index. Existing rows start empty and are filled by `embedsync run`
-- in the background (a re-scan that reuses the stored vectors, so nothing
-- is re-embedded); until a generation's rows are complete, filtered and
-- hybrid requests against it answer 503 search_index_building, while plain
-- vector search keeps working.
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
        EXECUTE format('ALTER TABLE {{.Schema}}.%I ADD COLUMN IF NOT EXISTS tsv tsvector', r.chunk_table);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON {{.Schema}}.%I USING gin (meta)', r.chunk_table || '_meta', r.chunk_table);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON {{.Schema}}.%I USING gin (tsv)', r.chunk_table || '_tsv', r.chunk_table);
    END LOOP;
END
$$;

RESET ROLE;
