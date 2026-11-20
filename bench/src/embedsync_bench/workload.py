"""Reproducible write workload and an independent freshness reader.

The writer commits one source transaction per operation at a fixed open-loop
rate. Every insert/update writes a unique token into the title, so a reader
that only sees what search sees (the sink chunk table, through the read-only
role) can tell exactly which committed version is searchable.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import psycopg

VOCAB = (
    "river mountain search tomato quartz marathon signal vector index replica latency ocean garden copper "
    "violin harbor lantern glacier meadow falcon orbit cedar canyon ember prism thunder willow basalt "
    "citrus nebula pepper saffron tundra velvet walnut yarrow zephyr acorn bramble cobalt dune fjord "
    "granite heron iris jasper kelp lagoon maple nectar opal pebble quill raven sierra thistle umber"
).split()


def body(rng: random.Random, lo: int = 60, hi: int = 110) -> str:
    return " ".join(rng.choice(VOCAB) for _ in range(rng.randint(lo, hi)))


def render(title: str, text: str) -> str:
    """The same rendering as the benchmark config template: "{{.title}}\\n\\n{{.body}}"."""
    return f"{title}\n\n{text}"


def preload(dsn: str, n: int, seed: int) -> None:
    rng = random.Random(seed)
    rows = [(i, f"doc {i} rev init", body(rng)) for i in range(1, n + 1)]
    with psycopg.connect(dsn) as c, c.cursor() as cur:
        cur.execute("TRUNCATE public.documents")
        cur.executemany("INSERT INTO public.documents (id, title, body) VALUES (%s, %s, %s)", rows)


@dataclass
class Event:
    seq: int
    op: str  # insert | update | delete
    key: int
    token: str | None
    commit_ts: float
    visible_ts: float | None = None


class Workload(threading.Thread):
    """Open-loop writer: op i is scheduled at start + i/rate (it catches up if late)."""

    def __init__(
        self,
        dsn: str,
        rate: float,
        duration: float,
        seed: int,
        live_keys: list[int],
        next_key: int,
        on_commit: Callable[[Event], None],
        mix=(0.6, 0.25, 0.15),
    ):
        super().__init__(daemon=True)
        self.dsn, self.rate, self.duration, self.seed = dsn, rate, duration, seed
        self.live = list(live_keys)
        self.next_key = next_key
        self.on_commit = on_commit
        self.mix = mix
        self.events: list[Event] = []
        self.errors = 0
        self.started = self.finished = 0.0
        self.stop_flag = threading.Event()

    def run(self) -> None:
        rng = random.Random(self.seed)
        with psycopg.connect(self.dsn, autocommit=True) as c:
            self.started = time.time()
            i = 0
            while not self.stop_flag.is_set():
                due = self.started + i / self.rate
                if due - self.started >= self.duration:
                    break
                delay = due - time.time()
                if delay > 0:
                    time.sleep(delay)
                r = rng.random()
                seq = i
                i += 1
                token = f"t{self.seed}x{seq}"
                try:
                    if r < self.mix[0] and self.live:
                        k = rng.choice(self.live)
                        with c.transaction():
                            c.execute(
                                "UPDATE public.documents SET title = %s, body = %s WHERE id = %s",
                                (f"doc {k} rev {token}", body(rng), k),
                            )
                        ev = Event(seq, "update", k, token, time.time())
                    elif r < self.mix[0] + self.mix[1] or not self.live:
                        k = self.next_key
                        self.next_key += 1
                        with c.transaction():
                            c.execute(
                                "INSERT INTO public.documents (id, title, body) VALUES (%s, %s, %s)",
                                (k, f"doc {k} rev {token}", body(rng)),
                            )
                        self.live.append(k)
                        ev = Event(seq, "insert", k, token, time.time())
                    else:
                        idx = rng.randrange(len(self.live))
                        k = self.live[idx]
                        self.live[idx] = self.live[-1]
                        self.live.pop()
                        with c.transaction():
                            c.execute("DELETE FROM public.documents WHERE id = %s", (k,))
                        ev = Event(seq, "delete", k, None, time.time())
                except psycopg.Error:
                    self.errors += 1
                    continue
                self.events.append(ev)
                self.on_commit(ev)
            self.finished = time.time()


class Reader(threading.Thread):
    """Independent reader: polls the sink table the search path reads.

    A pending event becomes visible when the sink shows that version or a newer
    one of the same row (a later version can coalesce an earlier one). The
    observation time is the start of the polling statement (its snapshot), so
    the measured latency over-estimates by at most one poll interval.
    """

    def __init__(self, dsn: str, table: Callable[[psycopg.Connection], str], poll: float = 0.02):
        super().__init__(daemon=True)
        self.dsn, self.table_fn, self.poll = dsn, table, poll
        self.lock = threading.Lock()
        self.pending: dict[int, list[Event]] = {}
        self.stop_flag = threading.Event()
        self.queries = 0
        self.errors = 0

    def expect(self, ev: Event) -> None:
        with self.lock:
            self.pending.setdefault(ev.key, []).append(ev)

    def outstanding(self) -> int:
        with self.lock:
            return sum(len(v) for v in self.pending.values())

    def run(self) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as c:
            table, refreshed = self.table_fn(c), time.time()
            while not self.stop_flag.is_set():
                if time.time() - refreshed > 2:
                    table, refreshed = self.table_fn(c), time.time()
                with self.lock:
                    keys = list(self.pending.keys())[:20000]
                if not keys:
                    time.sleep(self.poll)
                    continue
                t0 = time.time()
                try:
                    rows = c.execute(
                        f"SELECT source_key, content FROM {table} WHERE chunk_no = 0 AND source_key = ANY(%s)",
                        ([str(k) for k in keys],),
                    ).fetchall()
                except psycopg.Error:
                    self.errors += 1
                    time.sleep(self.poll)
                    continue
                self.queries += 1
                seen = {int(k): " " + " ".join(content.split()) + " " for k, content in rows}
                with self.lock:
                    for k in keys:
                        evs = self.pending.get(k)
                        if not evs:
                            continue
                        j = -1
                        content = seen.get(k)
                        for idx in range(len(evs) - 1, -1, -1):
                            ev = evs[idx]
                            if content is None and ev.op == "delete":
                                j = idx
                                break
                            if content is not None and ev.token and f" {ev.token} " in content:
                                j = idx
                                break
                        if j >= 0:
                            for ev in evs[: j + 1]:
                                ev.visible_ts = t0
                            rest = evs[j + 1 :]
                            if rest:
                                self.pending[k] = rest
                            else:
                                del self.pending[k]
                elapsed = time.time() - t0
                if elapsed < self.poll:
                    time.sleep(self.poll - elapsed)


def serving_table(schema: str = "embedsync") -> Callable[[psycopg.Connection], str]:
    def fn(c: psycopg.Connection) -> str:
        row = c.execute(
            f"SELECT g.chunk_table FROM {schema}.serving s JOIN {schema}.generations g ON g.id = s.generation_id "
            "WHERE s.name = 'default'"
        ).fetchone()
        return f'{schema}."{row[0]}"'

    return fn


def fixed_table(name: str) -> Callable[[psycopg.Connection], str]:
    return lambda _c: name
