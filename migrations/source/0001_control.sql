-- Source-side objects. Applied by an administrator (table owner) once.
-- The replicator role gets exactly: REPLICATION (granted by the DBA), SELECT on
-- the key and allowlisted columns, and write access to the marker table.
-- `embedsync init` re-applies the grants and the publication's table list on
-- every run, so pipelines added later are covered too.
CREATE SCHEMA IF NOT EXISTS {{.ControlSchema}};

CREATE TABLE IF NOT EXISTS {{.ControlSchema}}.schema_migrations (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

-- Watermark and lifecycle markers. Rows are written by embedsync and consumed
-- from the same logical replication stream as source changes, which is what
-- orders snapshot windows and generation starts relative to row changes.
CREATE TABLE IF NOT EXISTS {{.ControlSchema}}.markers (
    id            bigserial PRIMARY KEY,
    kind          text NOT NULL CHECK (kind IN ('start', 'low', 'high', 'retire', 'barrier')),
    generation_id text NOT NULL,
    job_id        bigint,
    window_id     uuid,
    created_at    timestamptz NOT NULL DEFAULT now()
);

REVOKE ALL ON SCHEMA {{.ControlSchema}} FROM PUBLIC;
GRANT USAGE ON SCHEMA {{.ControlSchema}} TO {{.ReplRole}};
GRANT SELECT, INSERT, DELETE ON {{.ControlSchema}}.markers TO {{.ReplRole}};
GRANT USAGE ON SEQUENCE {{.ControlSchema}}.markers_id_seq TO {{.ReplRole}};

-- Column-level grants: the replicator cannot read columns outside the allowlist.
{{.Grants}}

-- Publication with column lists: unlisted columns never leave the server.
-- Internal sink tables are deliberately absent to prevent feedback loops.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '{{.Publication}}') THEN
        CREATE PUBLICATION {{.Publication}}
            FOR TABLE {{.PubTables}}, {{.ControlSchema}}.markers
            WITH (publish = 'insert, update, delete, truncate');
    END IF;
END
$$;
