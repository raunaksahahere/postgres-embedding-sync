#!/usr/bin/env bash
# Build a .deb from an assembled payload:
#   build-deb.sh <payload_dir> <version> <variant> <deb_arch: amd64|arm64> <out_dir>
# Installs to /opt/embedsync with /usr/bin/embedsync on PATH. The three variants
# are separate packages that replace each other (only one is installed).
set -euo pipefail
PAYLOAD=$1 VERSION=$2 VARIANT=$3 ARCH=$4 OUT=$5
pkg="embedsync-${VARIANT}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/opt" "$work/usr/bin" "$work/DEBIAN" "$OUT"
cp -R "$PAYLOAD" "$work/opt/embedsync"
ln -s /opt/embedsync/embedsync "$work/usr/bin/embedsync"
size=$(du -sk "$work/opt" | cut -f1)
others=$(for v in nomodel minilm full; do [ "$v" != "$VARIANT" ] && printf 'embedsync-%s, ' "$v"; done)
cat > "$work/DEBIAN/control" <<CTRL
Package: ${pkg}
Version: ${VERSION}
Architecture: ${ARCH}
Maintainer: raunaksahahere <249486855+raunaksahahere@users.noreply.github.com>
Installed-Size: ${size}
Section: database
Priority: optional
Homepage: https://github.com/raunaksahahere/postgres-embedding-sync
Provides: embedsync
Conflicts: ${others%, }
Replaces: ${others%, }
Description: keep pgvector embeddings in sync with a PostgreSQL table
 embedsync follows a table through logical replication, re-embeds only what
 changed and writes vectors to pgvector transactionally. This package bundles
 the CPU embedding service (variant: ${VARIANT}). Run 'embedsync setup'.
CTRL
cat > "$work/DEBIAN/postinst" <<'SH'
#!/bin/sh
set -e
echo "embedsync installed. Next: run 'embedsync setup' in a terminal."
SH
chmod 0755 "$work/DEBIAN/postinst"
deb="$OUT/embedsync-${VERSION}-${VARIANT}-linux-${ARCH}.deb"
dpkg-deb --root-owner-group -Zxz -z6 --build "$work" "$deb" >/dev/null
echo "built $deb ($(du -h "$deb" | cut -f1))"
