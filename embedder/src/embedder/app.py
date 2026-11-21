"""FastAPI embedding service.

Serves an allowlist of sentence-transformers models, each pinned to an exact
Hugging Face commit. Requests name both the model and the revision; a request
for anything else is refused, so a client can never silently get vectors from
a different encoder.

Inputs are never logged. Per-input rejections (HTTP 422 with indexes) let the
pipeline quarantine exactly the offending rows; overload and outages surface as
5xx/connection errors, which the pipeline retries.

Environment:
  EMBEDDER_MODELS          comma-separated name@revision allowlist (required)
  EMBEDDER_MAX_INPUT_CHARS per-input character limit (default 8192)
  EMBEDDER_MAX_BATCH       max inputs per request (default 256)
  EMBEDDER_THREADS         torch CPU threads (default: torch's choice)
  HF_HOME                  model cache directory (first run downloads here)
  HF_HUB_OFFLINE=1         reuse the cache without network access
  EMBEDDER_BUNDLED_MODELS_DIR  read-only models shipped with an installer, laid out as
                           <dir>/<org>__<name>/<revision>/; used before the HF cache
  EMBEDDER_PARENT_PID      exit when this process ends (set by `embedsync` when it
                           starts the embedder itself)
  EMBEDDER_FAULT_REJECT_SUBSTRING  test-only: reject inputs containing it
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

log = logging.getLogger("embedder")


@dataclass
class Settings:
    models: list[tuple[str, str]]
    max_input_chars: int
    max_batch: int
    threads: int | None
    fault_reject: str | None

    @staticmethod
    def from_env() -> Settings:
        raw = os.environ.get("EMBEDDER_MODELS", "")
        models = []
        for item in filter(None, (p.strip() for p in raw.split(","))):
            name, sep, rev = item.rpartition("@")
            if not sep or not name or len(rev) < 7:
                raise ValueError(f"EMBEDDER_MODELS entry {item!r} must be name@revision (pinned commit)")
            models.append((name, rev))
        threads = os.environ.get("EMBEDDER_THREADS")
        return Settings(
            models=models,
            max_input_chars=int(os.environ.get("EMBEDDER_MAX_INPUT_CHARS", "8192")),
            max_batch=int(os.environ.get("EMBEDDER_MAX_BATCH", "256")),
            threads=int(threads) if threads else None,
            fault_reject=os.environ.get("EMBEDDER_FAULT_REJECT_SUBSTRING") or None,
        )


class EmbedRequest(BaseModel):
    model: str
    revision: str
    kind: Literal["document", "query"] = "document"
    inputs: list[str] = Field(min_length=1)


class EmbedResponse(BaseModel):
    model: str
    revision: str
    dimension: int
    normalized: bool
    embeddings: list[list[float]]


class Registry:
    """Loaded encoders keyed by (name, revision)."""

    def __init__(self) -> None:
        self.models: dict[tuple[str, str], object] = {}
        self.locks: dict[tuple[str, str], threading.Lock] = {}
        self.ready = False
        self.error: str | None = None

    def load(self, settings: Settings, loader=None) -> None:
        if loader is None:
            loader = _load_sentence_transformer
        try:
            for name, rev in settings.models:
                t0 = time.monotonic()
                self.models[(name, rev)] = loader(name, rev)
                self.locks[(name, rev)] = threading.Lock()
                log.info("loaded model %s@%s in %.1fs", name, rev, time.monotonic() - t0)
            self.ready = True
        except Exception as exc:  # surfaced via /healthz
            self.error = f"{type(exc).__name__}: {exc}"
            log.exception("model load failed")


def bundled_path(name: str, revision: str, root: str | None = None) -> str | None:
    """Directory of a model shipped with the installer, if present."""
    root = root if root is not None else os.environ.get("EMBEDDER_BUNDLED_MODELS_DIR")
    if not root:
        return None
    path = os.path.join(root, name.replace("/", "__"), revision)
    return path if os.path.isfile(os.path.join(path, "modules.json")) else None


def _load_sentence_transformer(name: str, revision: str):
    from sentence_transformers import SentenceTransformer

    local = bundled_path(name, revision)
    if local:
        log.info("using bundled model %s@%s", name, revision)
        return SentenceTransformer(local, device="cpu", local_files_only=True)
    return SentenceTransformer(name, revision=revision, device="cpu")


def _watch_parent(pid: int, interval: float = 2.0) -> None:
    """Exit when the process that started us is gone (no orphaned embedders)."""

    def alive() -> bool:
        if os.name == "nt":
            import ctypes

            k32 = ctypes.windll.kernel32
            h = k32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if not h:
                return False
            code = ctypes.c_ulong()
            ok = k32.GetExitCodeProcess(h, ctypes.byref(code))
            k32.CloseHandle(h)
            return bool(ok) and code.value == 259  # STILL_ACTIVE
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return os.getppid() == pid

    def loop() -> None:
        while alive():
            time.sleep(interval)
        log.info("parent process %d exited; stopping", pid)
        os._exit(0)

    threading.Thread(target=loop, daemon=True).start()


def create_app(settings: Settings | None = None, loader=None, background: bool = True) -> FastAPI:
    settings = settings or Settings.from_env()
    registry = Registry()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if settings.threads:
            import torch

            torch.set_num_threads(settings.threads)
        if background:
            threading.Thread(target=registry.load, args=(settings, loader), daemon=True).start()
        else:
            registry.load(settings, loader)
        yield

    app = FastAPI(title="embedsync embedder", version="0.2.0", lifespan=lifespan)
    app.state.registry = registry

    @app.get("/healthz")
    def healthz():
        if registry.error:
            raise HTTPException(status_code=500, detail=registry.error)
        if not registry.ready:
            raise HTTPException(status_code=503, detail="loading models")
        return {"status": "ok", "models": [f"{n}@{r}" for n, r in registry.models]}

    @app.get("/v1/models")
    def models():
        out = []
        for (name, rev), m in registry.models.items():
            out.append({"model": name, "revision": rev, "dimension": _dimension(m)})
        return {"models": out, "ready": registry.ready}

    @app.post("/v1/embed", response_model=EmbedResponse)
    def embed(req: EmbedRequest):
        if not registry.ready:
            raise HTTPException(status_code=503, detail="loading models")
        key = (req.model, req.revision)
        model = registry.models.get(key)
        if model is None:
            raise HTTPException(status_code=404, detail="model@revision not served")
        if len(req.inputs) > settings.max_batch:
            raise HTTPException(status_code=413, detail=f"batch larger than {settings.max_batch}")
        rejected = [
            i
            for i, text in enumerate(req.inputs)
            if len(text) > settings.max_input_chars
            or (settings.fault_reject is not None and settings.fault_reject in text)
        ]
        if rejected:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": f"input exceeds {settings.max_input_chars} characters or is not accepted",
                    "rejected": rejected,
                },
            )
        with registry.locks[key]:
            vecs = model.encode(
                req.inputs,
                batch_size=32,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        return EmbedResponse(
            model=req.model,
            revision=req.revision,
            dimension=int(vecs.shape[1]),
            normalized=True,
            embeddings=vecs.astype("float32").tolist(),
        )

    return app


def _dimension(model) -> int:
    if hasattr(model, "get_embedding_dimension"):
        return int(model.get_embedding_dimension())
    return int(model.get_sentence_embedding_dimension())


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parent = os.environ.get("EMBEDDER_PARENT_PID")
    if parent:
        _watch_parent(int(parent))
    uvicorn.run(
        create_app(),
        host=os.environ.get("EMBEDDER_HOST", "0.0.0.0"),
        port=int(os.environ.get("EMBEDDER_PORT", "8000")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
