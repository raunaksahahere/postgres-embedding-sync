#!/usr/bin/env bash
# Assemble one installer payload directory:
#   assemble.sh <version> <variant> <goos> <goarch> <embedder_dist_dir> <models_dir> <out_dir>
# <models_dir> is the output of fetch_models.py for that variant.
set -euo pipefail
VERSION=$1 VARIANT=$2 GOOS_=$3 GOARCH_=$4 EMB=$5 MODELS=$6 OUT=$7
PY="$(command -v python3 || command -v python)"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
rm -rf "$OUT" && mkdir -p "$OUT"
exe=embedsync
[ "$GOOS_" = windows ] && exe=embedsync.exe
(cd "$ROOT" && CGO_ENABLED=0 GOOS="$GOOS_" GOARCH="$GOARCH_" go build -trimpath \
  -ldflags "-s -w -buildid= -X github.com/raunaksahahere/postgres-embedding-sync/internal/version.Version=${VERSION}" \
  -o "$OUT/$exe" ./cmd/embedsync)
cp -R "$EMB" "$OUT/embedder"
mkdir -p "$OUT/models"
cp -R "$MODELS"/. "$OUT/models/"
cp "$ROOT/LICENSE" "$ROOT/NOTICE" "$OUT/"
cp "$ROOT/examples/embedsync.yaml" "$OUT/embedsync.example.yaml"
models=$("$PY" - "$OUT/models/manifest.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))["models"]
print("\n".join(f"  - {x['name']} (included)" for x in m) if m else
      "  - no model included: setup downloads any Hugging Face model you choose")
PY
)
"$PY" - "$ROOT/packaging/README.txt" "$OUT/README.txt" "$VERSION" "$VARIANT" "$models" "$GOOS_" <<'PY'
import sys
src, dst, ver, var, models, goos = sys.argv[1:]
t = open(src).read().replace("@VERSION@", ver).replace("@VARIANT@", var).replace("@MODELS@", models)
open(dst, "w", newline="\r\n" if goos == "windows" else "\n").write(t)
PY
echo "assembled $OUT ($(du -sh "$OUT" | cut -f1))"
