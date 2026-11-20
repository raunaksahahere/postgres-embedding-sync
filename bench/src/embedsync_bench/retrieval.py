"""Retrieval quality and live model cutover on BEIR SciFact.

1. Download SciFact (pinned revision, SHA-256 verified; not redistributed).
2. Sample 500 judged queries with a fixed seed (held out: nothing is tuned on them).
3. Load the 5,183-document corpus into the source table; embedsync builds the
   initial generation (all-MiniLM-L6-v2).
4. While a reader searches continuously and a writer keeps changing rows:
   - migrate --to bge        (expected: gates pass, promoted)
   - migrate --to minilm_l3  (expected: rejected, bge keeps serving)
   - migrate rollback        (bge -> minilm, within the rollback window)
The quality numbers (recall@10, nDCG@10, paired bootstrap CIs, ANN vs exact)
come from embedsync's own gate reports; the gates were declared in
bench/embedsync.bench.yaml before any run.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import random
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg

from .common import DATA, Stack, environment, log, percentiles
from .freshness import start_stack
from .workload import Workload

SCIFACT_REV = "984eed826375f18d27936c4a32bf0f8491e3f414"  # BeIR/scifact (pre-Parquet revision)
QRELS_REV = "2938d17dc3b09882fdb8c12bbbe2e2dc0e75a029"  # BeIR/scifact-qrels
FILES = {
    "corpus.jsonl.gz": (
        f"https://huggingface.co/datasets/BeIR/scifact/resolve/{SCIFACT_REV}/corpus.jsonl.gz",
        "2f4a2a264485e5b5b95ae9cd5534817447757423991a4767d6da811539c0853e",
    ),
    "queries.jsonl.gz": (
        f"https://huggingface.co/datasets/BeIR/scifact/resolve/{SCIFACT_REV}/queries.jsonl.gz",
        "c29574b9cb8fdf043df8494f689de15f4604ac4b585eb34bda9b6a389aa6c4d3",
    ),
    "qrels-test.tsv": (
        f"https://huggingface.co/datasets/BeIR/scifact-qrels/resolve/{QRELS_REV}/test.tsv",
        "0864bb985e0ca2367ba217977e72004d549054b2b06666ed9d4825ac7c21284c",
    ),
    "qrels-train.tsv": (
        f"https://huggingface.co/datasets/BeIR/scifact-qrels/resolve/{QRELS_REV}/train.tsv",
        "a53f2114831916c096b6c37d9e54da68cef4efdcdbd5ed46533601af972acf1d",
    ),
}


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def fetch(dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    for name, (url, digest) in FILES.items():
        p = dest / name
        if not p.exists() or sha256(p) != digest:
            log(f"downloading {name}")
            with urllib.request.urlopen(url, timeout=120) as r:
                p.write_bytes(r.read())
        if sha256(p) != digest:
            raise RuntimeError(f"{name}: checksum mismatch")
    return dest


def build_eval(src: Path, out: Path, n: int, seed: int) -> dict:
    """Sample n judged queries (train+test qrels) with a fixed seed."""
    qrels: dict[str, dict[str, int]] = {}
    for name in ("qrels-test.tsv", "qrels-train.tsv"):
        for line in (src / name).read_text().splitlines()[1:]:
            qid, did, score = line.split("\t")
            if int(score) > 0:
                qrels.setdefault(qid, {})[did] = int(score)
    queries = {}
    with gzip.open(src / "queries.jsonl.gz", "rt") as f:
        for line in f:
            q = json.loads(line)
            queries[q["_id"]] = q["text"]
    judged = sorted((q for q in qrels if q in queries), key=lambda x: int(x))
    sample = sorted(random.Random(seed).sample(judged, n), key=lambda x: int(x))
    out.mkdir(parents=True, exist_ok=True)
    with (out / "queries.jsonl").open("w") as f:
        for qid in sample:
            f.write(json.dumps({"_id": qid, "text": queries[qid]}) + "\n")
    with (out / "qrels.tsv").open("w") as f:
        f.write("query-id\tcorpus-id\tscore\n")
        for qid in sample:
            for did, s in sorted(qrels[qid].items()):
                f.write(f"{qid}\t{did}\t{s}\n")
    return {
        "judged_queries": len(judged),
        "sampled": n,
        "seed": seed,
        "queries_sha256": sha256(out / "queries.jsonl"),
        "qrels_sha256": sha256(out / "qrels.tsv"),
        "query_texts": [queries[q] for q in sample],
    }


def corpus_loader(src: Path):
    def load(dsn: str) -> None:
        rows = []
        with gzip.open(src / "corpus.jsonl.gz", "rt") as f:
            for line in f:
                d = json.loads(line)
                rows.append((int(d["_id"]), d.get("title") or "", d.get("text") or ""))
        log(f"loading {len(rows)} SciFact documents")
        with psycopg.connect(dsn) as c, c.cursor() as cur:
            cur.execute("TRUNCATE public.documents")
            cur.executemany("INSERT INTO public.documents (id, title, body) VALUES (%s, %s, %s)", rows)

    return load


class SearchReader(threading.Thread):
    """Searches the serving generation continuously through embedsync's /search."""

    def __init__(self, base: str, queries: list[str], seed: int, pause: float = 0.05):
        super().__init__(daemon=True)
        self.base, self.queries, self.pause = base, queries, pause
        self.rng = random.Random(seed)
        self.stop_flag = threading.Event()
        self.samples: list[tuple[float, float, str | None, str | None]] = []  # t, ms, generation, error

    def run(self) -> None:
        while not self.stop_flag.is_set():
            q = self.rng.choice(self.queries)
            t0 = time.time()
            gen = err = None
            try:
                url = f"{self.base}/search?k=10&q={urllib.parse.quote(q[:4000])}"
                with urllib.request.urlopen(url, timeout=30) as r:
                    body = json.loads(r.read())
                gen = body.get("generation")
                if not body.get("hits"):
                    err = "no hits"
            except Exception as e:  # noqa: BLE001 - every failure is recorded
                err = type(e).__name__ + ": " + str(e)[:200]
            self.samples.append((t0, (time.time() - t0) * 1000, gen, err))
            self.stop_flag.wait(self.pause)


def migrate(stack: Stack, *args: str) -> tuple[int, dict | None, str, float, float]:
    t0 = time.time()
    p = stack.embedsync("migrate", *args, check=False)
    t1 = time.time()
    rep = None
    out = p.stdout.strip()
    if out.startswith("{"):
        try:
            rep = json.loads(out)
        except json.JSONDecodeError:
            rep = None
    log(f"migrate {' '.join(args)} -> exit {p.returncode} in {t1 - t0:.1f}s")
    return p.returncode, rep, (p.stderr or "")[-4000:], t0, t1


def switches(samples, t_base: float) -> list[dict]:
    """Generation changes seen by the reader: last answer from the old one and
    first answer from the new one (the gap bounds the visible cutover time)."""
    out = []
    prev = None
    for t, _ms, gen, err in samples:
        if err or gen is None:
            continue
        if prev is not None and gen != prev[1]:
            out.append(
                {
                    "from": prev[1],
                    "to": gen,
                    "last_old_t": round(prev[0] - t_base, 3),
                    "first_new_t": round(t - t_base, 3),
                    "gap_ms": round((t - prev[0]) * 1000, 1),
                }
            )
        prev = (t, gen)
    return out


def run(stack: Stack, n_queries: int, seed: int, write_rate: float) -> dict:
    src = fetch(DATA / "scifact")
    eval_dir = DATA / "scifact-eval"
    manifest = build_eval(src, eval_dir, n_queries, seed)
    texts = manifest.pop("query_texts")
    stack.eval_dir = eval_dir
    t_build = time.time()
    start_stack(stack, seed, loader=corpus_loader(src))
    initial_build_s = time.time() - t_build
    env = environment(stack)

    reader = SearchReader(stack.embedsync_url, texts, seed)
    state_live: list[int] = []
    writer = Workload(stack.admin_dsn, write_rate, 24 * 3600, seed, state_live, 1_000_000_000, lambda _e: None)
    t_base = time.time()
    reader.start()
    writer.start()
    time.sleep(10)

    steps = []
    for label, args, expect in (
        ("promote_bge", ("--to", "bge", "--json"), 0),
        ("reject_minilm_l3", ("--to", "minilm_l3", "--json"), 3),
        ("rollback", ("rollback",), 0),
    ):
        code, rep, err, t0, t1 = migrate(stack, *args)
        steps.append(
            {
                "step": label,
                "exit_code": code,
                "expected_exit_code": expect,
                "ok": code == expect,
                "started_t": round(t0 - t_base, 2),
                "finished_t": round(t1 - t_base, 2),
                "gate_report": rep,
                "stderr_tail": err if code != expect else None,
            }
        )
        time.sleep(10)
    reader.stop_flag.set()
    writer.stop_flag.set()
    reader.join(timeout=30)
    writer.join(timeout=30)
    status = json.loads(stack.embedsync("migrate", "status").stdout or "[]")

    lat = [ms for _t, ms, _g, e in reader.samples if not e]
    errors = [(round(t - t_base, 2), e) for t, _ms, _g, e in reader.samples if e]
    return {
        "environment": env,
        "dataset": {
            "name": "BEIR SciFact",
            "corpus_documents": 5183,
            "revision": SCIFACT_REV,
            "qrels_revision": QRELS_REV,
            **manifest,
        },
        "declared_gates_file": "bench/embedsync.bench.yaml",
        "initial_build_s": round(initial_build_s, 1),
        "steps": steps,
        "reader": {
            "searches": len(reader.samples),
            "errors": len(errors),
            "error_examples": errors[:10],
            "latency_ms": percentiles(lat),
            "switches": switches(reader.samples, t_base),
        },
        "writer": {"rate_rows_per_s": write_rate, "committed": len(writer.events), "errors": writer.errors},
        "migrations": status,
        "verdict": {
            "promotion_passed": steps[0]["ok"],
            "regression_rejected": steps[1]["ok"],
            "rollback_ok": steps[2]["ok"],
            "reader_errors": len(errors),
        },
    }
