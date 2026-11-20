#!/usr/bin/env bash
# Build release archives and checksums into dist/:
#   embedsync_<version>_<os>_<arch>.tar.gz  (binary, LICENSE, README.md, example config)
#   SHA256SUMS
set -euo pipefail
VERSION="${1:?usage: release-build.sh <version>}"
cd "$(dirname "$0")/.."
rm -rf dist && mkdir -p dist
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  os="${target%/*}" arch="${target#*/}"
  name="embedsync_${VERSION}_${os}_${arch}"
  stage="dist/${name}"
  mkdir -p "$stage"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w -buildid= -X github.com/raunaksahahere/postgres-embedding-sync/internal/version.Version=${VERSION}" \
    -o "$stage/embedsync" ./cmd/embedsync
  cp LICENSE README.md examples/embedsync.yaml "$stage/"
  tar -C dist --sort=name --mtime='2026-01-01 00:00:00Z' --owner=0 --group=0 --numeric-owner \
    -czf "dist/${name}.tar.gz" "$name"
  rm -rf "$stage"
done
(cd dist && sha256sum ./*.tar.gz | sed 's# \./# #' > SHA256SUMS)
cat dist/SHA256SUMS
