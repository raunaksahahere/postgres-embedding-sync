-- Demo source tables for examples/embedsync-views.yaml and a small seeded
-- dataset (written for this project, CC0). Two tenants use the same numeric
-- ids on purpose: every key and reference starts with tenant_id, so their
-- documents, sections and tags never mix.
CREATE TABLE IF NOT EXISTS public.kb_documents (
    tenant_id  bigint NOT NULL,
    id         bigint NOT NULL,
    title      text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS public.kb_sections (
    tenant_id   bigint NOT NULL,
    id          bigint NOT NULL,
    document_id bigint NOT NULL,
    position    integer,                 -- NULL sorts last
    heading     text NOT NULL,
    body        text NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES public.kb_documents (tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.kb_tags (
    tenant_id bigint NOT NULL,
    id        bigint NOT NULL,
    name      text NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS public.kb_document_tags (
    tenant_id   bigint NOT NULL,
    document_id bigint NOT NULL,
    tag_id      bigint NOT NULL,
    PRIMARY KEY (tenant_id, document_id, tag_id),
    FOREIGN KEY (tenant_id, document_id) REFERENCES public.kb_documents (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, tag_id) REFERENCES public.kb_tags (tenant_id, id) ON DELETE CASCADE
);

INSERT INTO public.kb_documents (tenant_id, id, title) VALUES
    (1, 1, 'Replication slots'),
    (1, 2, 'Choosing a primary key'),
    (1, 3, 'Monitoring freshness'),
    (2, 1, 'Sourdough starter'),
    (2, 2, 'Proofing bread dough'),
    (2, 3, 'Storing flour')
ON CONFLICT (tenant_id, id) DO NOTHING;

INSERT INTO public.kb_sections (tenant_id, id, document_id, position, heading, body) VALUES
    (1, 1, 1, 1, 'What a slot keeps', 'A logical replication slot keeps every WAL segment its consumer has not confirmed.'),
    (1, 2, 1, 2, 'Disk usage', 'If the consumer stops, retained WAL grows until max_slot_wal_keep_size invalidates the slot.'),
    (1, 3, 2, 1, 'Stable keys', 'Stable primary keys make change data capture simple.'),
    (1, 4, 2, NULL, 'Footnote', 'Sections without a position are rendered after the ordered ones.'),
    (1, 5, 3, 1, 'Measure lag', 'Measure the time from a committed change to the moment it becomes searchable.'),
    (2, 1, 1, 1, 'Feeding', 'Feed the starter equal weights of flour and water daily.'),
    (2, 2, 1, 2, 'Temperature', 'Keep it at room temperature until it doubles within six hours.'),
    (2, 3, 2, 1, 'When it is ready', 'Dough is proofed when it springs back slowly when pressed.'),
    (2, 4, 3, 1, 'Containers', 'Store flour in an airtight container in a cool, dark place.')
ON CONFLICT (tenant_id, id) DO NOTHING;

INSERT INTO public.kb_tags (tenant_id, id, name) VALUES
    (1, 1, 'postgres'),
    (1, 2, 'operations'),
    (2, 1, 'baking'),
    (2, 2, 'storage')
ON CONFLICT (tenant_id, id) DO NOTHING;

-- Tag 1 is shared by every document of its tenant: renaming it re-renders
-- all three documents of that tenant and none of the other tenant's.
INSERT INTO public.kb_document_tags (tenant_id, document_id, tag_id) VALUES
    (1, 1, 1),
    (1, 1, 2),
    (1, 2, 1),
    (1, 3, 1),
    (1, 3, 2),
    (2, 1, 1),
    (2, 2, 1),
    (2, 3, 1),
    (2, 3, 2)
ON CONFLICT (tenant_id, document_id, tag_id) DO NOTHING;
