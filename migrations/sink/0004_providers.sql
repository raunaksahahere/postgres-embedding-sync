-- Which embedding provider computed a generation's vectors, and how it was
-- asked (request dimensions, document/query input types). Queries are
-- routed by the generation they search, so after promotion or rollback the
-- query vector always comes from the same provider and settings as the
-- documents. Existing generations were built by the built-in embedder.
SET ROLE {{.SinkRole}};

ALTER TABLE {{.Schema}}.generations ADD COLUMN IF NOT EXISTS provider jsonb NOT NULL DEFAULT '{"name": "local", "kind": "local"}';

RESET ROLE;
