import gzip
import json

from embedsync_bench.common import percentiles
from embedsync_bench.retrieval import build_eval, switches
from embedsync_bench.workload import Event, Reader, render


def test_percentiles():
    p = percentiles([float(x) for x in range(1, 101)])
    assert p["n"] == 100 and p["p50"] == 50.5 and p["max"] == 100.0
    assert percentiles([])["p99"] is None


def test_switches_reports_gap():
    s = [(0.0, 5, "a", None), (1.0, 5, "a", None), (1.2, 5, None, "boom"), (1.5, 5, "b", None), (2.0, 5, "a", None)]
    sw = switches(s, 0.0)
    assert [(x["from"], x["to"]) for x in sw] == [("a", "b"), ("b", "a")]
    assert sw[0]["gap_ms"] == 500.0


def test_build_eval_is_seeded(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    with gzip.open(src / "queries.jsonl.gz", "wt") as f:
        for i in range(20):
            f.write(json.dumps({"_id": str(i), "text": f"q{i}"}) + "\n")
    rows = "".join(f"{i}\t{100 + i}\t1\n" for i in range(20))
    (src / "qrels-test.tsv").write_text("query-id\tcorpus-id\tscore\n" + rows)
    (src / "qrels-train.tsv").write_text("query-id\tcorpus-id\tscore\n")
    a = build_eval(src, tmp_path / "a", 5, seed=1)
    b = build_eval(src, tmp_path / "b", 5, seed=1)
    assert a["queries_sha256"] == b["queries_sha256"] and a["sampled"] == 5
    assert (tmp_path / "a" / "qrels.tsv").read_text().startswith("query-id\tcorpus-id\tscore\n")


def test_reader_marks_coalesced_versions_visible():
    r = Reader("unused", lambda _c: "t")
    e1 = Event(1, "update", 7, "t1x1", 10.0)
    e2 = Event(2, "update", 7, "t1x2", 11.0)
    r.expect(e1)
    r.expect(e2)
    assert r.outstanding() == 2
    # Simulate one poll result where only the newer version is visible.
    content = " " + " ".join(render("doc 7 rev t1x2", "body words").split()) + " "
    with r.lock:
        evs = r.pending[7]
        j = max(i for i, ev in enumerate(evs) if ev.token and f" {ev.token} " in content)
        for ev in evs[: j + 1]:
            ev.visible_ts = 12.0
        del r.pending[7]
    assert e1.visible_ts == e2.visible_ts == 12.0
