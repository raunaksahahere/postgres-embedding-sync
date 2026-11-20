"""Baselines measured with the same workload and independent reader.

trigger_queue
    The trigger + queue + scheduled worker pattern (as in Supabase's
    "automatic embeddings" guide: triggers enqueue changed row ids, a pg_cron
    job runs every 10 seconds and embeds queued rows). This is a faithful
    re-implementation of the pattern for comparison, not Supabase itself: the
    queue is a plain table drained with FOR UPDATE SKIP LOCKED and the worker is
    a Python loop on a 10 s tick calling the same local embedder.

periodic_batch
    A periodic re-embed job (cron-style) that scans the table every interval,
    re-embeds rows whose content hash changed and deletes rows that vanished.
"""

from __future__ import annotations

import hashlib
import threading
import time

import psycopg

from .common import http_json
from .workload import render

TQ_SETUP = """
DROP SCHEMA IF EXISTS bench_tq CASCADE;
CREATE SCHEMA bench_tq;
CREATE TABLE bench_tq.queue (id bigserial PRIMARY KEY, source_key bigint NOT NULL,
                             enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE bench_tq.chunks (source_key text PRIMARY KEY, chunk_no int NOT NULL DEFAULT 0,
                              content text NOT NULL, embedding vector(384) NOT NULL,
                              updated_at timestamptz NOT NULL DEFAULT now());
CREATE FUNCTION bench_tq.enqueue() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO bench_tq.queue (source_key) VALUES (CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END);
  IF TG_OP = 'UPDATE' AND OLD.id <> NEW.id THEN
    INSERT INTO bench_tq.queue (source_key) VALUES (OLD.id);
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER bench_tq_enqueue AFTER INSERT OR DELETE OR UPDATE OF id, title, body ON public.documents
  FOR EACH ROW EXECUTE FUNCTION bench_tq.enqueue();
GRANT USAGE ON SCHEMA bench_tq TO embedsync_reader;
GRANT SELECT ON bench_tq.chunks TO embedsync_reader;
"""

TQ_TEARDOWN = "DROP TRIGGER IF EXISTS bench_tq_enqueue ON public.documents; DROP SCHEMA IF EXISTS bench_tq CASCADE;"

PB_SETUP = """
DROP SCHEMA IF EXISTS bench_pb CASCADE;
CREATE SCHEMA bench_pb;
CREATE TABLE bench_pb.chunks (source_key text PRIMARY KEY, chunk_no int NOT NULL DEFAULT 0,
                              content text NOT NULL, content_hash text NOT NULL, embedding vector(384) NOT NULL,
                              updated_at timestamptz NOT NULL DEFAULT now());
GRANT USAGE ON SCHEMA bench_pb TO embedsync_reader;
GRANT SELECT ON bench_pb.chunks TO embedsync_reader;
"""

PB_TEARDOWN = "DROP SCHEMA IF EXISTS bench_pb CASCADE;"


def vec(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.7g}" for x in v) + "]"


class Embedder:
    def __init__(self, url: str, model: str, revision: str, batch: int = 32):
        self.url, self.model, self.revision, self.batch = url, model, revision, batch
        self.calls = 0
        self.embedded = 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), self.batch):
            part = texts[i : i + self.batch]
            r = http_json(
                self.url + "/v1/embed",
                {"model": self.model, "revision": self.revision, "kind": "document", "inputs": part},
                timeout=120,
            )
            out.extend(r["embeddings"])
            self.calls += 1
            self.embedded += len(part)
        return out


class TriggerQueueWorker(threading.Thread):
    """Every `interval` seconds, drain the queue in batches and apply current row content."""

    def __init__(self, dsn: str, emb: Embedder, interval: float = 10.0, batch: int = 64):
        super().__init__(daemon=True)
        self.dsn, self.emb, self.interval, self.batch = dsn, emb, interval, batch
        self.stop_flag = threading.Event()
        self.ticks = 0

    def drain(self, c: psycopg.Connection) -> int:
        total = 0
        while True:
            with c.transaction():
                keys = [
                    r[0]
                    for r in c.execute(
                        "DELETE FROM bench_tq.queue WHERE id IN (SELECT id FROM bench_tq.queue ORDER BY id "
                        "LIMIT %s FOR UPDATE SKIP LOCKED) RETURNING source_key",
                        (self.batch,),
                    ).fetchall()
                ]
                if not keys:
                    return total
                uniq = sorted(set(keys))
                rows = c.execute("SELECT id, title, body FROM public.documents WHERE id = ANY(%s)", (uniq,)).fetchall()
                present = {r[0]: render(r[1], r[2]) for r in rows}
                gone = [str(k) for k in uniq if k not in present]
                if present:
                    ids = list(present)
                    texts = [present[k] for k in ids]
                    vecs = self.emb.embed(texts)
                    c.execute(
                        "INSERT INTO bench_tq.chunks (source_key, content, embedding) "
                        "SELECT k, t, v::vector FROM unnest(%s::text[], %s::text[], %s::text[]) AS u(k, t, v) "
                        "ON CONFLICT (source_key) DO UPDATE SET content = EXCLUDED.content, "
                        "embedding = EXCLUDED.embedding, updated_at = now()",
                        ([str(k) for k in ids], texts, [vec(v) for v in vecs]),
                    )
                if gone:
                    c.execute("DELETE FROM bench_tq.chunks WHERE source_key = ANY(%s)", (gone,))
                total += len(uniq)

    def run(self) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            next_tick = time.time()
            while not self.stop_flag.is_set():
                delay = next_tick - time.time()
                if delay > 0 and self.stop_flag.wait(delay):
                    break
                next_tick += self.interval
                self.drain(c)
                self.ticks += 1


class PeriodicBatchWorker(threading.Thread):
    """Every `interval` seconds, diff the whole table against the sink by content hash."""

    def __init__(self, dsn: str, emb: Embedder, interval: float = 60.0):
        super().__init__(daemon=True)
        self.dsn, self.emb, self.interval = dsn, emb, interval
        self.stop_flag = threading.Event()
        self.ticks = 0

    def sync(self, c: psycopg.Connection) -> int:
        rows = c.execute("SELECT id, title, body FROM public.documents").fetchall()
        src = {str(r[0]): render(r[1], r[2]) for r in rows}
        have = dict(c.execute("SELECT source_key, content_hash FROM bench_pb.chunks").fetchall())
        changed = [k for k, t in src.items() if have.get(k) != hashlib.sha256(t.encode()).hexdigest()]
        gone = [k for k in have if k not in src]
        if changed:
            texts = [src[k] for k in changed]
            vecs = self.emb.embed(texts)
            with c.transaction():
                c.execute(
                    "INSERT INTO bench_pb.chunks (source_key, content, content_hash, embedding) "
                    "SELECT k, t, h, v::vector FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[]) "
                    "AS u(k, t, h, v) ON CONFLICT (source_key) DO UPDATE SET content = EXCLUDED.content, "
                    "content_hash = EXCLUDED.content_hash, embedding = EXCLUDED.embedding, updated_at = now()",
                    (changed, texts, [hashlib.sha256(t.encode()).hexdigest() for t in texts], [vec(v) for v in vecs]),
                )
        if gone:
            c.execute("DELETE FROM bench_pb.chunks WHERE source_key = ANY(%s)", (gone,))
        return len(changed) + len(gone)

    def run(self) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            next_tick = time.time()
            while not self.stop_flag.is_set():
                delay = next_tick - time.time()
                if delay > 0 and self.stop_flag.wait(delay):
                    break
                next_tick += self.interval
                self.sync(c)
                self.ticks += 1
