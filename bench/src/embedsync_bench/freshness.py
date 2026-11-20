"""Commit-to-searchable freshness: embedsync at several write rates, an outage
with WAL growth, and the trigger+queue and periodic-batch baselines.

Every number comes from the independent reader (workload.Reader), which sees
only what the search path sees: the serving generation's chunk table through
the read-only role.
"""

from __future__ import annotations

import threading
import time

import psycopg

from . import baselines
from .common import (
    MODELS,
    Stack,
    environment,
    log,
    metric_sum,
    percentiles,
    prom_scrape,
    wait_until,
)
from .workload import Reader, Workload, fixed_table, preload, serving_table

PRELOAD = 2000


def _ready(stack: Stack) -> bool:
    try:
        with psycopg.connect(stack.admin_dsn) as c:
            r = c.execute(
                "SELECT count(*) FILTER (WHERE state IN ('pending','running')), count(*) FROM embedsync.backfill_jobs"
            ).fetchone()
            g = c.execute("SELECT count(*) FROM embedsync.generations WHERE state IN ('ready','active')").fetchone()
        return r[1] > 0 and r[0] == 0 and g[0] > 0
    except psycopg.Error:
        return False


def start_stack(stack: Stack, seed: int, preload_rows: int = PRELOAD, loader=None) -> None:
    log("resetting benchmark stack")
    stack.reset()
    stack.up("postgres", "embedder")
    stack.wait_pg()
    if loader is None:
        log(f"preloading {preload_rows} rows (seed {seed})")
        preload(stack.admin_dsn, preload_rows, seed)
    else:
        loader(stack.admin_dsn)
    stack.compose("up", "-d", "--build", "init", "embedsync")
    t0 = time.time()
    wait_until("initial backfill", lambda: _ready(stack), timeout=1800, every=2)
    log(f"initial backfill done in {time.time() - t0:.1f}s")


def measure(
    stack: Stack,
    name: str,
    table_fn,
    rate: float,
    duration: float,
    seed: int,
    state: dict,
    drain_timeout: float = 300,
    on_tick=None,
    scrape: bool = True,
) -> dict:
    reader = Reader(stack.reader_dsn, table_fn)
    reader.start()
    m0 = prom_scrape(stack.embedsync_url) if scrape else {}
    w = Workload(stack.admin_dsn, rate, duration, seed, state["live"], state["next_key"], reader.expect)
    w.start()
    while w.is_alive():
        if on_tick:
            on_tick(time.time() - w.started if w.started else 0.0)
        w.join(timeout=1.0)
    write_end = time.time()
    try:
        wait_until(f"{name} drain", lambda: reader.outstanding() == 0, timeout=drain_timeout, every=0.2)
    except TimeoutError:
        log(f"{name}: {reader.outstanding()} events never became visible within {drain_timeout}s")
    drain_end = time.time()
    m1 = prom_scrape(stack.embedsync_url) if scrape else {}
    reader.stop_flag.set()
    reader.join(timeout=5)
    state["live"], state["next_key"] = w.live, w.next_key
    lat = [ev.visible_ts - ev.commit_ts for ev in w.events if ev.visible_ts is not None]
    ops = {k: sum(1 for e in w.events if e.op == k) for k in ("insert", "update", "delete")}
    span = max(w.finished - w.started, 1e-9)
    out = {
        "system": name,
        "target_rate_rows_per_s": rate,
        "duration_s": duration,
        "seed": seed,
        "committed_writes": len(w.events),
        "achieved_rate_rows_per_s": round(len(w.events) / span, 2),
        "ops": ops,
        "writer_errors": w.errors,
        "never_visible": sum(1 for e in w.events if e.visible_ts is None),
        "latency_s": percentiles(lat),
        "drain_after_last_write_s": round(drain_end - write_end, 2),
        "reader_polls": reader.queries,
        "reader_errors": reader.errors,
        "events": [
            [
                round(e.commit_ts - w.started, 3),
                e.op,
                None if e.visible_ts is None else round(e.visible_ts - e.commit_ts, 4),
            ]
            for e in w.events
        ],
    }
    if scrape:
        busy = max(drain_end - w.started, 1e-9)
        emb = metric_sum(m1, "embedsync_embeddings_total") - metric_sum(m0, "embedsync_embeddings_total")
        cnt = metric_sum(m1, "embedsync_commit_to_searchable_seconds_count") - metric_sum(
            m0, "embedsync_commit_to_searchable_seconds_count"
        )
        tot = metric_sum(m1, "embedsync_commit_to_searchable_seconds_sum") - metric_sum(
            m0, "embedsync_commit_to_searchable_seconds_sum"
        )
        out["embeddings"] = int(emb)
        out["embeddings_per_s"] = round(emb / busy, 2)
        out["internal_mean_commit_to_durable_s"] = round(tot / cnt, 4) if cnt else None
        out["source_transactions"] = int(cnt)
    lat_s = out["latency_s"]
    log(
        f"{name} @ {rate}/s: n={lat_s['n']} p50={lat_s['p50']}s p99={lat_s['p99']}s "
        f"never_visible={out['never_visible']}"
    )
    return out


def freshness(stack: Stack, rates: list[float], duration: float, seed: int) -> dict:
    start_stack(stack, seed)
    env = environment(stack)
    state = {"live": list(range(1, PRELOAD + 1)), "next_key": PRELOAD + 1}
    runs = []
    for i, rate in enumerate(rates):
        runs.append(measure(stack, "embedsync", serving_table(), rate, duration, seed + i, state))
    return {"environment": env, "preloaded_rows": PRELOAD, "runs": runs}


def _slot_sample(c: psycopg.Connection) -> tuple[int | None, str | None]:
    r = c.execute(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, wal_status "
        "FROM pg_replication_slots WHERE slot_name = 'embedsync_slot'"
    ).fetchone()
    return (int(r[0]), r[1]) if r and r[0] is not None else (None, None)


class Ballast(threading.Thread):
    """Unpublished WAL traffic: the slot must retain it during an outage and
    release it (via keepalive idle checkpoints) afterwards."""

    def __init__(self, dsn: str, rows_per_s: float = 20):
        super().__init__(daemon=True)
        self.dsn, self.rate = dsn, rows_per_s
        self.stop_flag = threading.Event()

    def run(self) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            c.execute("CREATE TABLE IF NOT EXISTS public.bench_ballast (id bigserial PRIMARY KEY, payload text)")
            c.execute("ALTER TABLE public.bench_ballast ALTER COLUMN payload SET STORAGE EXTERNAL")
            while not self.stop_flag.wait(1 / self.rate):
                c.execute(
                    "INSERT INTO public.bench_ballast (payload) "
                    "SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 3000)"
                )
                c.execute("DELETE FROM public.bench_ballast WHERE id < currval('public.bench_ballast_id_seq') - 50")


def outage(stack: Stack, rate: float, seed: int, before: float, down: float, after: float) -> dict:
    start_stack(stack, seed)
    env = environment(stack)
    state = {"live": list(range(1, PRELOAD + 1)), "next_key": PRELOAD + 1}
    samples: list[dict] = []
    marks: dict[str, float] = {}
    stop = threading.Event()
    t_start = time.time()

    def sampler() -> None:
        with psycopg.connect(stack.admin_dsn, autocommit=True) as c:
            while not stop.wait(1.0):
                retained, status = _slot_sample(c)
                try:
                    m = prom_scrape(stack.embedsync_url)
                except OSError:
                    m = {}
                cs = next(
                    (k.split('"')[1] for k, v in m.items() if k.startswith("embedsync_consumer_state{") and v == 1),
                    None,
                )
                samples.append(
                    {
                        "t": round(time.time() - t_start, 2),
                        "retained_wal_bytes": retained,
                        "wal_status": status,
                        "freshness_lag_s": m.get("embedsync_freshness_lag_seconds"),
                        "pending_transactions": m.get("embedsync_pending_transactions"),
                        "consumer_state": cs,
                    }
                )

    ballast = Ballast(stack.admin_dsn)
    ballast.start()
    st = threading.Thread(target=sampler, daemon=True)
    st.start()
    phase = {"stopped": False, "started": False}

    def tick(t: float) -> None:
        if not phase["stopped"] and t >= before:
            phase["stopped"] = True
            marks["embedder_stopped_t"] = round(time.time() - t_start, 2)
            log("outage: stopping embedder")
            stack.compose("stop", "embedder", capture=True)
        if phase["stopped"] and not phase["started"] and t >= before + down:
            phase["started"] = True
            marks["embedder_restart_t"] = round(time.time() - t_start, 2)
            log("outage: starting embedder")
            stack.compose("start", "embedder", capture=True)

    run = measure(
        stack, "embedsync", serving_table(), rate, before + down + after, seed, state, drain_timeout=600, on_tick=tick
    )
    marks["drained_t"] = round(time.time() - t_start, 2)
    time.sleep(15)  # observe retained WAL being released after recovery
    stop.set()
    st.join(timeout=5)
    ballast.stop_flag.set()
    ballast.join(timeout=5)
    retained = [s["retained_wal_bytes"] for s in samples if s["retained_wal_bytes"] is not None]
    return {
        "environment": env,
        "rate_rows_per_s": rate,
        "phases_s": {"before": before, "outage": down, "after": after},
        "marks": marks,
        "ballast": "unpublished table, ~20 rows/s of ~96 KB each",
        "max_retained_wal_bytes": max(retained) if retained else None,
        "final_retained_wal_bytes": retained[-1] if retained else None,
        "run": run,
        "samples": samples,
    }


def run_baselines(
    stack: Stack, rates: list[float], duration: float, seed: int, tq_interval: float, pb_interval: float
) -> dict:
    """Runs on a fresh stack with embedsync stopped so the baselines get the whole CPU."""
    start_stack(stack, seed)
    env = environment(stack)
    stack.compose("stop", "embedsync", capture=True)
    name, rev = MODELS["minilm"]
    out: dict = {"environment": env, "preloaded_rows": PRELOAD, "trigger_queue": [], "periodic_batch": []}

    for kind in ("trigger_queue", "periodic_batch"):
        # Each baseline starts from the same preloaded table contents.
        preload(stack.admin_dsn, PRELOAD, seed)
        state = {"live": list(range(1, PRELOAD + 1)), "next_key": PRELOAD + 1}
        emb = baselines.Embedder(stack.embedder_url, name, rev)
        with psycopg.connect(stack.admin_dsn, autocommit=True) as c:
            c.execute(baselines.TQ_SETUP if kind == "trigger_queue" else baselines.PB_SETUP)
        if kind == "trigger_queue":
            with psycopg.connect(stack.admin_dsn, autocommit=True) as c:
                c.execute("INSERT INTO bench_tq.queue (source_key) SELECT id FROM public.documents")
            worker = baselines.TriggerQueueWorker(stack.admin_dsn, emb, interval=tq_interval)
            table = fixed_table("bench_tq.chunks")
        else:
            worker = baselines.PeriodicBatchWorker(stack.admin_dsn, emb, interval=pb_interval)
            table = fixed_table("bench_pb.chunks")
        # Initial sync outside the measurement.
        with psycopg.connect(stack.admin_dsn, autocommit=True) as c:
            t0 = time.time()
            if kind == "trigger_queue":
                worker.drain(c)
            else:
                worker.sync(c)
            log(f"{kind}: initial sync {time.time() - t0:.1f}s")
        worker.start()
        for i, rate in enumerate(rates):
            emb0 = emb.embedded
            r = measure(
                stack,
                kind,
                table,
                rate,
                duration,
                seed + i,
                state,
                scrape=False,
                drain_timeout=max(300, 3 * pb_interval),
            )
            r["embeddings"] = emb.embedded - emb0
            r["interval_s"] = tq_interval if kind == "trigger_queue" else pb_interval
            out[kind].append(r)
        worker.stop_flag.set()
        worker.join(timeout=30)
        with psycopg.connect(stack.admin_dsn, autocommit=True) as c:
            c.execute(baselines.TQ_TEARDOWN if kind == "trigger_queue" else baselines.PB_TEARDOWN)
    return out


def summarize(fresh: dict | None, base: dict | None, out: dict | None) -> dict:
    rows = []
    for src, label in ((fresh, "embedsync"),):
        if src:
            for r in src["runs"]:
                rows.append(
                    {
                        "system": label,
                        "rate": r["target_rate_rows_per_s"],
                        **r["latency_s"],
                        "never_visible": r["never_visible"],
                        "embeddings_per_s": r.get("embeddings_per_s"),
                    }
                )
    if base:
        for kind in ("trigger_queue", "periodic_batch"):
            for r in base[kind]:
                rows.append(
                    {
                        "system": kind,
                        "rate": r["target_rate_rows_per_s"],
                        **r["latency_s"],
                        "never_visible": r["never_visible"],
                        "interval_s": r["interval_s"],
                    }
                )
    s: dict = {"latency_table": rows}
    if out:
        s["outage"] = {
            k: out[k]
            for k in ("rate_rows_per_s", "phases_s", "marks", "max_retained_wal_bytes", "final_retained_wal_bytes")
        }
        s["outage"]["latency_s"] = out["run"]["latency_s"]
        s["outage"]["never_visible"] = out["run"]["never_visible"]
    return s
