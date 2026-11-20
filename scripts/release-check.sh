#!/usr/bin/env bash
# Verify a set of release assets: every advertised archive exists, checksums
# match, and the linux/amd64 binary installs and reports the right version.
#   release-check.sh <version> [dir]   (dir defaults to dist/)
set -euo pipefail
VERSION="${1:?usage: release-check.sh <version> [dir]}"
DIR="${2:-$(dirname "$0")/../dist}"
cd "$DIR"
expected=(SHA256SUMS)
for t in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
  expected+=("embedsync_${VERSION}_${t}.tar.gz")
done
missing=0
for f in "${expected[@]}"; do
  if [ ! -s "$f" ]; then echo "MISSING asset: $f" >&2; missing=1; fi
done
[ "$missing" = 0 ] || exit 1
sha256sum -c SHA256SUMS
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tar -C "$tmp" -xzf "embedsync_${VERSION}_linux_amd64.tar.gz"
install -m 0755 "$tmp/embedsync_${VERSION}_linux_amd64/embedsync" "$tmp/embedsync"
got="$("$tmp/embedsync" version)"
[ "$got" = "embedsync ${VERSION}" ] || { echo "installed binary reports '$got', want 'embedsync ${VERSION}'" >&2; exit 1; }
"$tmp/embedsync" help >/dev/null
echo "release assets OK: ${#expected[@]} files, installed binary reports '$got'"
