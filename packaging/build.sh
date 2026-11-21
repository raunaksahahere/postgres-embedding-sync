#!/usr/bin/env bash
# Build the three installer variants for the current platform.
#   packaging/build.sh <version> <goos> <goarch> <embedder_dist_dir> <out_dir>
# Variants: nomodel (setup downloads the model you choose from Hugging Face),
# minilm (all-MiniLM-L6-v2 included), full (all three pinned models included).
set -euo pipefail
VERSION=$1 GOOS_=$2 GOARCH_=$3 EMB=$4 OUT=$5
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${WORK:-$ROOT/build/packaging}"
VARIANTS="${VARIANTS:-nomodel minilm full}"
mkdir -p "$WORK" "$OUT"
OUT="$(cd "$OUT" && pwd)"
for variant in $VARIANTS; do
  models="$WORK/models-$variant"
  if [ ! -f "$models/manifest.json" ]; then
    uv run --no-project --with huggingface-hub==2.4.0 python "$ROOT/packaging/fetch_models.py" "$models" "$variant"
  fi
  payload="$WORK/payload-$variant/embedsync"
  "$ROOT/packaging/assemble.sh" "$VERSION" "$variant" "$GOOS_" "$GOARCH_" "$EMB" "$models" "$payload"
  case "$GOOS_" in
    linux) "$ROOT/packaging/linux/build-deb.sh" "$payload" "$VERSION" "$variant" "$GOARCH_" "$OUT" ;;
    darwin) "$ROOT/packaging/macos/build-dmg.sh" "$payload" "$VERSION" "$variant" "$GOARCH_" "$OUT" ;;
    windows)
      iscc="${ISCC:-iscc}"
      "$iscc" //Q "//DVersion=$VERSION" "//DVariant=$variant" "//DSourceDir=$(cygpath -w "$payload")" \
        "//DOutputDir=$(cygpath -w "$OUT")" "$(cygpath -w "$ROOT/packaging/windows/embedsync.iss")"
      echo "built $OUT/embedsync-$VERSION-$variant-windows-x64-setup.exe" ;;
    *) echo "unsupported GOOS $GOOS_" >&2; exit 1 ;;
  esac
  rm -rf "$WORK/payload-$variant"
done
ls -l "$OUT"
