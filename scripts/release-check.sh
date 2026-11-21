#!/usr/bin/env bash
# Verify a set of release assets: every advertised file exists, checksums match,
# and the linux/amd64 CLI installs and reports the right version.
#   release-check.sh <version> [dir] [--cli-only]   (dir defaults to dist/)
# Without --cli-only the native installers (3 variants x .deb amd64/arm64,
# macOS .dmg, Windows setup .exe) must be present too.
set -euo pipefail
VERSION="${1:?usage: release-check.sh <version> [dir] [--cli-only]}"
DIR="${2:-$(dirname "$0")/../dist}"
CLI_ONLY="${3:-}"
cd "$DIR"
expected=(SHA256SUMS)
for t in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
  expected+=("embedsync_${VERSION}_${t}.tar.gz")
done
expected+=("embedsync_${VERSION}_windows_amd64.zip")
if [ "$CLI_ONLY" != --cli-only ]; then
  for v in nomodel minilm full; do
    expected+=("embedsync-${VERSION}-${v}-linux-amd64.deb" "embedsync-${VERSION}-${v}-linux-arm64.deb"
               "embedsync-${VERSION}-${v}-macos-arm64.dmg" "embedsync-${VERSION}-${v}-windows-x64-setup.exe")
  done
fi
missing=0
for f in "${expected[@]}"; do
  if [ ! -s "$f" ]; then echo "MISSING asset: $f" >&2; missing=1; fi
done
[ "$missing" = 0 ] || exit 1
sha256sum -c SHA256SUMS
for f in "${expected[@]}"; do
  [ "$f" = SHA256SUMS ] || grep -q " $f\$" SHA256SUMS || { echo "not in SHA256SUMS: $f" >&2; exit 1; }
done
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tar -C "$tmp" -xzf "embedsync_${VERSION}_linux_amd64.tar.gz"
install -m 0755 "$tmp/embedsync_${VERSION}_linux_amd64/embedsync" "$tmp/embedsync"
got="$("$tmp/embedsync" version)"
[ "$got" = "embedsync ${VERSION}" ] || { echo "installed binary reports '$got', want 'embedsync ${VERSION}'" >&2; exit 1; }
"$tmp/embedsync" help >/dev/null
echo "release assets OK: ${#expected[@]} files, installed binary reports '$got'"
