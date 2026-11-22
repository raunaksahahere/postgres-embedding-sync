-- Demo source table for examples/embedsync-search.yaml (written for this project, CC0).
-- Mirrors the table used by the search acceptance test. `internal` is not declared
-- as metadata or allowlisted, so it never reaches embedsync.
CREATE TABLE IF NOT EXISTS public.articles (
    id        bigint PRIMARY KEY,
    title     text NOT NULL,
    body      text NOT NULL,
    lang      text NOT NULL,
    year      int,
    tags      text[],
    published timestamptz,
    org       text,
    rating    float8,
    featured  boolean NOT NULL DEFAULT false,
    internal  text
);

INSERT INTO public.articles (id, title, body, lang, year, tags, published, org, rating, featured, internal) VALUES
    (1,  'Replication slots and disk usage', 'A logical replication slot keeps every WAL segment its consumer has not confirmed. Monitor retained WAL and free disk space.', 'en', 2021, '{postgres,replication}', '2021-03-02 09:00:00+00', 'org1', 4.2, false, 'draft 1'),
    (2,  'Choosing a primary key', 'Stable primary keys make change data capture simple. Natural keys that change turn an update into a delete and an insert downstream.', 'en', 2022, '{postgres,design}', '2022-06-14 12:30:00+00', 'org0', 3.9, true, 'draft 2'),
    (3,  'Sourdough starter basics', 'A sourdough starter is a culture of wild yeast and lactic acid bacteria. Feed it equal weights of flour and water every day.', 'en', 2019, '{baking}', '2019-09-21 08:15:00+00', 'org1', 4.8, false, 'draft 3'),
    (4,  'Sauerteig im Alltag', 'Ein Sauerteig braucht täglich frisches Mehl und Wasser. Bei Raumtemperatur verdoppelt er sein Volumen innerhalb weniger Stunden.', 'de', 2020, '{baking,alltag}', '2020-01-10 07:00:00+00', 'org0', 4.4, false, 'draft 4'),
    (5,  'Le levain au quotidien', 'Un levain se nourrit de farine et d eau chaque jour. Il est prêt quand il double de volume.', 'fr', 2023, '{baking}', '2023-04-03 10:00:00+00', 'org1', 4.1, false, 'draft 5'),
    (6,  'Watering tomato plants', 'Tomatoes prefer deep watering two or three times a week rather than a little every day. Water at the base to keep leaves dry.', 'en', 2018, '{garden}', '2018-07-01 06:45:00+00', 'org0', 3.5, false, 'draft 6'),
    (7,  'Pruning fruit trees', 'Prune apple and pear trees in late winter while they are dormant. Remove dead and crossing branches to open the canopy.', 'en', 2020, '{garden,orchard}', '2020-02-18 16:20:00+00', 'org1', 4.0, true, 'draft 7'),
    (8,  'Gartenarbeit im Frühling', 'Im Frühjahr werden Beete vorbereitet und Obstbäume geschnitten. Kompost verbessert die Erde.', 'de', 2021, '{garden}', '2021-05-08 09:30:00+00', 'org0', 3.7, false, 'draft 8'),
    (9,  'Jupiter''s four large moons', 'Through a small telescope the Galilean moons look like bright points lined up beside Jupiter. Their positions change from night to night.', 'en', 2017, '{astronomy}', '2017-11-30 21:00:00+00', 'org1', 4.6, false, 'draft 9'),
    (10, 'What is a light-year', 'A light-year is the distance light travels in one year, about 9.46 trillion kilometres. It measures distance, not time.', 'en', 2024, '{astronomy,physics}', '2024-08-12 11:00:00+00', 'org0', 4.3, true, 'draft 10'),
    (11, 'Training for a first marathon', 'Most beginner marathon plans last sixteen to twenty weeks and build the weekly long run gradually.', 'en', 2022, '{running}', '2022-03-05 06:30:00+00', 'org1', 3.8, false, 'draft 11'),
    (12, 'Marathon-Training für Einsteiger', 'Ein Trainingsplan dauert meist sechzehn bis zwanzig Wochen. Ruhetage gehören genauso dazu wie der lange Lauf.', 'de', 2023, '{running}', '2023-10-01 06:00:00+00', 'org0', 4.0, false, 'draft 12'),
    (13, 'Jet lag strategies', 'Shift your sleep schedule toward the destination time zone a few days before flying. Morning daylight helps reset the body clock.', 'en', 2019, '{travel}', '2019-04-22 14:10:00+00', 'org1', 3.6, false, 'draft 13'),
    (14, 'Packing light for travel', 'Choose clothes that layer and share a colour palette. A carry-on bag avoids lost luggage.', 'en', 2021, '{travel,packing}', '2021-08-27 18:45:00+05:30', 'org0', 4.1, false, 'draft 14'),
    (15, 'Index types in PostgreSQL', 'B-tree indexes serve equality and range queries, GIN suits full-text search and arrays, and HNSW accelerates approximate nearest-neighbour search.', 'en', 2024, '{postgres,indexing}', '2024-01-16 10:20:00+00', 'org1', 4.7, true, 'draft 15'),
    (16, 'HNSW recall and speed', 'An HNSW graph trades recall for speed. Raising ef_search visits more candidates and improves recall at the cost of latency.', 'en', 2023, '{postgres,indexing,vectors}', '2023-02-09 13:00:00+00', 'org0', 4.5, false, 'draft 16'),
    (17, 'Vacuum and table bloat', 'PostgreSQL marks updated and deleted rows as dead tuples. Long transactions and abandoned replication slots can stop autovacuum from cleaning up.', 'en', 2020, '{postgres,maintenance}', '2020-10-05 15:00:00+00', 'org1', 3.9, false, 'draft 17'),
    (18, 'Growing herbs indoors', 'Basil, parsley and mint grow well on a sunny windowsill. Let the soil surface dry slightly between waterings.', 'en', 2018, '{garden,herbs}', '2018-05-19 07:30:00+00', 'org0', 3.4, false, 'draft 18'),
    -- The order note: the only document containing the identifier SKU-48213.
    (19, 'order note', 'Order reference SKU-48213 was shipped on time from the Leeds warehouse.', 'en', 2023, '{orders}', '2023-05-01 00:00:00+00', 'org1', 1.0, false, 'draft 19'),
    (20, 'Road trip notes', 'My car broke down on the highway near the summit, so we spent the night at a small inn.', 'en', 2024, '{travel}', '2024-02-02 10:00:00+05:30', 'org0', 2.0, true, 'draft 20')
ON CONFLICT (id) DO NOTHING;
