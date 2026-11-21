#!/usr/bin/env bash
# Build a macOS installer: a .pkg (installs /usr/local/embedsync and the
# /usr/local/bin/embedsync command) inside a .dmg.
#   build-dmg.sh <payload_dir> <version> <variant> <arch: arm64> <out_dir>
# Unsigned: see the README for the one-time "Open Anyway" step.
set -euo pipefail
PAYLOAD=$1 VERSION=$2 VARIANT=$3 ARCH=$4 OUT=$5
HERE="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/root" "$work/scripts" "$work/dmg" "$OUT"
cp -R "$PAYLOAD" "$work/root/embedsync"
cp "$HERE/uninstall.sh" "$work/root/embedsync/uninstall.sh"
chmod 0755 "$work/root/embedsync/uninstall.sh"
cp "$HERE/postinstall" "$work/scripts/postinstall"
chmod 0755 "$work/scripts/postinstall"
pkgbuild --root "$work/root" --install-location /usr/local --scripts "$work/scripts" \
  --identifier io.github.raunaksahahere.embedsync --version "$VERSION" \
  --ownership recommended "$work/dmg/Install embedsync.pkg"
cp "$PAYLOAD/README.txt" "$work/dmg/README.txt"
cat > "$work/dmg/If macOS blocks the installer.txt" <<'TXT'
This installer is not signed by an Apple Developer ID, so macOS may refuse to
open it the first time ("Apple could not verify ...").

Either:
  1. Double-click "Install embedsync.pkg", click Done, then open
     System Settings > Privacy & Security, scroll down and click "Open Anyway"
     next to the embedsync message, and confirm.
or, in Terminal:
  2. sudo installer -pkg "/Volumes/embedsync/Install embedsync.pkg" -target /

Afterwards run `embedsync setup` in Terminal.
To uninstall: sudo /usr/local/embedsync/uninstall.sh
TXT
dmg="$OUT/embedsync-${VERSION}-${VARIANT}-macos-${ARCH}.dmg"
rm -f "$dmg"
hdiutil create -volname "embedsync" -srcfolder "$work/dmg" -fs HFS+ -format UDZO -ov "$dmg" >/dev/null
echo "built $dmg ($(du -h "$dmg" | cut -f1))"
