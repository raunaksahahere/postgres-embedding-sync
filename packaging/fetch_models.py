"""Download pinned models into the installer layout <out>/<org>__<name>/<revision>/
and write <out>/manifest.json describing them (read by `embedsync setup`).

usage: fetch_models.py <out_dir> <variant>     variant: nomodel | minilm | full
"""

import json
import sys
from pathlib import Path

from huggingface_hub import snapshot_download

# key, name, revision, dimension, query_prefix -- must match examples/embedsync.yaml
MODELS = [
    ("minilm", "sentence-transformers/all-MiniLM-L6-v2", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41", 384, ""),
    (
        "bge",
        "BAAI/bge-small-en-v1.5",
        "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
        384,
        "Represent this sentence for searching relevant passages: ",
    ),
    (
        "minilm_l3",
        "sentence-transformers/paraphrase-MiniLM-L3-v2",
        "4ca70771034acceecb2e72475f72050fcdde4ddc",
        384,
        "",
    ),
]
VARIANTS = {"nomodel": [], "minilm": ["minilm"], "full": ["minilm", "bge", "minilm_l3"]}
# Only what sentence-transformers needs to load a PyTorch model from a directory.
ALLOW = ["*.json", "*.txt", "model.safetensors", "1_Pooling/*", "2_Normalize/*", "*.model"]
IGNORE = ["onnx/*", "openvino/*"]


def main() -> None:
    out, variant = Path(sys.argv[1]), sys.argv[2]
    keys = VARIANTS[variant]
    out.mkdir(parents=True, exist_ok=True)
    manifest = []
    for key, name, rev, dim, prefix in MODELS:
        if key not in keys:
            continue
        dest = out / name.replace("/", "__") / rev
        snapshot_download(name, revision=rev, local_dir=dest, allow_patterns=ALLOW, ignore_patterns=IGNORE)
        cache = dest / ".cache"
        if cache.exists():
            import shutil

            shutil.rmtree(cache)
        manifest.append({"key": key, "name": name, "revision": rev, "dimension": dim, "query_prefix": prefix})
        print(f"bundled {name}@{rev[:10]} -> {dest}")
    (out / "manifest.json").write_text(json.dumps({"variant": variant, "models": manifest}, indent=2) + "\n")


if __name__ == "__main__":
    main()
