"""embedsync-bench: one command per suite, machine-readable results with seeds.

  embedsync-bench freshness  [--rates 5,15,30,60] [--duration 60] [--seed 7]
  embedsync-bench outage     [--rate 15] [--before 30] [--down 60] [--after 60]
  embedsync-bench baselines  [--rates 5,15,30] [--duration 60]
  embedsync-bench retrieval  [--queries 500] [--seed 20261120]
  embedsync-bench chart
  embedsync-bench all

Results go to bench/results/<suite>.json. Every run starts from a fresh
benchmark Compose project (embedsync-bench) and leaves it running for
inspection; `docker compose -p embedsync-bench down -v` removes it.
"""

from __future__ import annotations

import argparse
import shlex
import sys
import time

from . import chart, freshness, retrieval
from .common import Stack, log, write_result


def _rates(s: str) -> list[float]:
    return [float(x) for x in s.split(",") if x]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="embedsync-bench", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("suite", choices=["freshness", "outage", "baselines", "retrieval", "chart", "all"])
    ap.add_argument("--rates", type=_rates, default=[5, 15, 30, 60])
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--rate", type=float, default=15)
    ap.add_argument("--before", type=float, default=30)
    ap.add_argument("--down", type=float, default=60)
    ap.add_argument("--after", type=float, default=60)
    ap.add_argument("--tq-interval", type=float, default=10)
    ap.add_argument("--pb-interval", type=float, default=60)
    ap.add_argument("--queries", type=int, default=500)
    ap.add_argument("--eval-seed", type=int, default=20261120)
    ap.add_argument("--write-rate", type=float, default=2)
    ap.add_argument("--pg-major", default="17")
    ap.add_argument("--embedder-threads", type=int, default=4, help="torch CPU threads in the embedder")
    a = ap.parse_args(argv)
    stack = Stack(pg_major=a.pg_major, embedder_threads=a.embedder_threads)
    cmd = "embedsync-bench " + " ".join(shlex.quote(x) for x in (argv if argv is not None else sys.argv[1:]))
    meta = {"command": cmd, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "synthetic_workload": True}
    suites = ["freshness", "outage", "baselines", "retrieval"] if a.suite == "all" else [a.suite]
    for suite in suites:
        log(f"=== {suite}")
        if suite == "freshness":
            r = freshness.freshness(stack, a.rates, a.duration, a.seed)
        elif suite == "outage":
            r = freshness.outage(stack, a.rate, a.seed, a.before, a.down, a.after)
        elif suite == "baselines":
            r = freshness.run_baselines(stack, a.rates, a.duration, a.seed, a.tq_interval, a.pb_interval)
        elif suite == "retrieval":
            r = retrieval.run(stack, a.queries, a.eval_seed, a.write_rate)
            meta = {
                **meta,
                "synthetic_workload": False,
                "note": "real dataset (BEIR SciFact) and real pinned models; the concurrent writer is synthetic",
            }
        else:
            chart.main()
            continue
        write_result(suite, {"meta": {**meta, "suite": suite}, **r})
    if a.suite == "all":
        chart.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
