"""Shared helpers: the benchmark Compose stack, connections, environment capture, results."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

REPO = Path(__file__).resolve().parents[3]
RESULTS = REPO / "bench" / "results"
DATA = REPO / "bench" / ".data"

MODELS = {
    "minilm": ("sentence-transformers/all-MiniLM-L6-v2", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"),
    "bge": ("BAAI/bge-small-en-v1.5", "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a"),
    "minilm_l3": ("sentence-transformers/paraphrase-MiniLM-L3-v2", "4ca70771034acceecb2e72475f72050fcdde4ddc"),
}


_PGV = "pgvector/pgvector:0.8.7-pg{}-bookworm@sha256:{}"
PG_IMAGES = {
    "16": _PGV.format(16, "484e838076e04939d27d3e176e91c86eea96236126023be8d3059761122e579f"),
    "17": _PGV.format(17, "b513c48566e4777cd3370451400000c78eda6a08131d68f4c31d4a76f27bd8fd"),
    "18": _PGV.format(18, "38681dc6eaf55308e2a4c6de392d131652dcfac70e1263e35cd9b5ea3fe39782"),
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


@dataclass
class Stack:
    """The benchmark Compose project (separate ports and volumes from the quickstart)."""

    project: str = "embedsync-bench"
    pg_port: int = 56432
    embedder_port: int = 58100
    embedsync_port: int = 59287
    eval_dir: Path | None = None
    pg_major: str = "17"
    embedder_threads: int = 4
    env: dict[str, str] = field(default_factory=dict)

    def _env(self) -> dict[str, str]:
        e = dict(os.environ)
        e.update(
            PG_PORT=str(self.pg_port),
            EMBEDDER_PORT=str(self.embedder_port),
            EMBEDSYNC_PORT=str(self.embedsync_port),
            PG_IMAGE=PG_IMAGES[self.pg_major],
            EMBEDDER_THREADS=str(self.embedder_threads),
        )
        if self.eval_dir is not None:
            e["BENCH_EVAL_DIR"] = str(self.eval_dir)
        e.update(self.env)
        return e

    def compose(self, *args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
        cmd = [
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(REPO / "docker-compose.yml"),
            "-f",
            str(REPO / "bench" / "compose.bench.yml"),
            *args,
        ]
        return subprocess.run(cmd, cwd=REPO, env=self._env(), check=check, text=True, capture_output=capture)

    def reset(self) -> None:
        """Remove the project's containers and database volume (the model cache is kept)."""
        self.compose("down", "--volumes", "--remove-orphans", check=False, capture=True)

    def up(self, *services: str) -> None:
        self.compose("up", "-d", "--build", "--wait", *services)

    def embedsync(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return self.compose("exec", "-T", "embedsync", "embedsync", *args, check=check, capture=True)

    @property
    def admin_dsn(self) -> str:
        pw = os.environ.get("POSTGRES_PASSWORD", "demo-admin")
        return f"postgresql://postgres:{pw}@127.0.0.1:{self.pg_port}/app"

    @property
    def reader_dsn(self) -> str:
        pw = os.environ.get("EMBEDSYNC_READER_PASSWORD", "demo-reader")
        return f"postgresql://embedsync_reader:{pw}@127.0.0.1:{self.pg_port}/app"

    @property
    def embedder_url(self) -> str:
        return f"http://127.0.0.1:{self.embedder_port}"

    @property
    def embedsync_url(self) -> str:
        return f"http://127.0.0.1:{self.embedsync_port}"

    def wait_pg(self, timeout: float = 60) -> None:
        deadline = time.time() + timeout
        while True:
            try:
                with psycopg.connect(self.admin_dsn, connect_timeout=2) as c:
                    c.execute("SELECT 1")
                return
            except psycopg.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(0.5)


def http_json(url: str, body: dict | None = None, timeout: float = 30) -> dict:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def prom_scrape(url: str) -> dict[str, float]:
    """Parse a Prometheus text exposition into {series: value} (no histograms buckets needed)."""
    out: dict[str, float] = {}
    with urllib.request.urlopen(url + "/metrics", timeout=10) as r:
        for line in r.read().decode().splitlines():
            if not line or line.startswith("#"):
                continue
            name, _, val = line.rpartition(" ")
            try:
                out[name] = float(val)
            except ValueError:
                continue
    return out


def metric_sum(m: dict[str, float], prefix: str) -> float:
    return sum(v for k, v in m.items() if k == prefix or k.startswith(prefix + "{"))


def wait_until(what: str, fn, timeout: float, every: float = 1.0):
    deadline = time.time() + timeout
    while True:
        v = fn()
        if v:
            return v
        if time.time() > deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(every)


def percentiles(xs: list[float]) -> dict[str, float | int | None]:
    import numpy as np

    if not xs:
        return {"n": 0, "p50": None, "p90": None, "p99": None, "max": None, "mean": None}
    a = np.asarray(xs, dtype=float)
    return {
        "n": int(a.size),
        "p50": round(float(np.percentile(a, 50)), 4),
        "p90": round(float(np.percentile(a, 90)), 4),
        "p99": round(float(np.percentile(a, 99)), 4),
        "max": round(float(a.max()), 4),
        "mean": round(float(a.mean()), 4),
    }


def _cmd(*args: str) -> str:
    try:
        return subprocess.run(args, check=True, text=True, capture_output=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def environment(stack: Stack) -> dict:
    """Hardware and software versions recorded with every result."""
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        cpu = platform.processor()
    mem_gb = None
    try:
        m = re.search(r"MemTotal:\s+(\d+) kB", Path("/proc/meminfo").read_text())
        mem_gb = round(int(m.group(1)) / 1024 / 1024, 1) if m else None
    except OSError:
        pass
    pg = vec = ""
    try:
        with psycopg.connect(stack.admin_dsn) as c:
            pg = c.execute("SHOW server_version").fetchone()[0]
            vec = c.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'").fetchone()[0]
    except Exception:  # noqa: BLE001 - best effort
        pass
    es = stack.embedsync("version", check=False)
    return {
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_gb": mem_gb,
        "os": f"{platform.system()} {platform.release()}",
        "docker": _cmd("docker", "version", "--format", "{{.Server.Version}}"),
        "postgres": pg,
        "pgvector": vec,
        "embedsync": (es.stdout or "").strip(),
        "git_commit": _cmd("git", "-C", str(REPO), "rev-parse", "HEAD") or None,
        "python": platform.python_version(),
        "embedding_device": "cpu",
        "embedder_torch_threads": stack.embedder_threads,
        "models": {k: {"name": n, "revision": r} for k, (n, r) in MODELS.items()},
    }


def write_result(name: str, payload: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    p = RESULTS / f"{name}.json"
    p.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    log(f"wrote {p.relative_to(REPO)}")
    return p
