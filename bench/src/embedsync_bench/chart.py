"""Charts from bench/results/*.json (PNG + SVG next to the data)."""

from __future__ import annotations

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .common import RESULTS, log  # noqa: E402

COLORS = {"embedsync": "#2b6cb0", "trigger_queue": "#c05621", "periodic_batch": "#718096"}
LABELS = {
    "embedsync": "embedsync (CDC)",
    "trigger_queue": "trigger + queue (10 s tick)",
    "periodic_batch": "periodic batch (60 s)",
}


def _load(name: str) -> dict | None:
    p = RESULTS / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def _save(fig, name: str) -> None:
    for ext in ("png", "svg"):
        fig.savefig(RESULTS / f"{name}.{ext}", dpi=140, bbox_inches="tight")
    plt.close(fig)
    log(f"wrote bench/results/{name}.png/.svg")


def latency_chart() -> None:
    fresh, base = _load("freshness"), _load("baselines")
    if not fresh:
        return
    series = {"embedsync": fresh["runs"]}
    if base:
        series["trigger_queue"] = base["trigger_queue"]
        series["periodic_batch"] = base["periodic_batch"]
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    for name, runs in series.items():
        rates = [r["target_rate_rows_per_s"] for r in runs]
        p50 = [r["latency_s"]["p50"] for r in runs]
        p99 = [r["latency_s"]["p99"] for r in runs]
        ax.plot(rates, p99, "-o", color=COLORS[name], label=f"{LABELS[name]} p99")
        ax.plot(rates, p50, "--", color=COLORS[name], alpha=0.7, label=f"{LABELS[name]} p50")
    ax.set_yscale("log")
    ax.set_xlabel("write rate (rows/s, one row per transaction)")
    ax.set_ylabel("commit → searchable (s, log scale)")
    ax.set_title("Freshness measured by an independent reader")
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(fontsize=7.5, loc="upper left", ncol=1)
    _save(fig, "freshness_latency")


def outage_chart() -> None:
    out = _load("outage")
    if not out:
        return
    s = out["samples"]
    t = [x["t"] for x in s]
    wal = [(x["retained_wal_bytes"] or 0) / 2**20 for x in s]
    lag = [x["freshness_lag_s"] or 0 for x in s]
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.5, 5.4), sharex=True)
    m = out["marks"]
    for ax in (ax1, ax2):
        if "embedder_stopped_t" in m and "embedder_restart_t" in m:
            ax.axvspan(
                m["embedder_stopped_t"],
                m["embedder_restart_t"],
                color="#e53e3e",
                alpha=0.12,
                label="embedding service down",
            )
        ax.grid(True, alpha=0.25)
    ax1.plot(t, wal, color="#2b6cb0")
    ax1.set_ylabel("WAL retained by slot (MiB)")
    ax1.legend(fontsize=8, loc="upper left")
    ax1.set_title(f"Embedding outage at {out['rate_rows_per_s']} rows/s plus unpublished WAL ballast")
    # Per-write latency by commit time (from the independent reader).
    ev = out["run"]["events"]
    t0 = out["marks"].get("embedder_stopped_t", 0) - out["phases_s"]["before"]
    xs = [e[0] + t0 for e in ev if e[2] is not None]
    ys = [e[2] for e in ev if e[2] is not None]
    ax2.scatter(xs, ys, s=4, color="#2b6cb0", alpha=0.6, label="commit → searchable per write")
    ax2.plot(t, lag, color="#c05621", lw=1, label="embedsync_freshness_lag_seconds")
    ax2.set_ylabel("seconds")
    ax2.set_xlabel("time since start (s)")
    ax2.legend(fontsize=8, loc="upper left")
    _save(fig, "outage_lag")


def main() -> None:
    latency_chart()
    outage_chart()
