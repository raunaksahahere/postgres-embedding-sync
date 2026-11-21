#!/usr/bin/env bash
# Build the CLI-only release archives and checksums into dist/:
#   embedsync_<version>_<os>_<arch>.tar.gz  (linux, darwin)
#   embedsync_<version>_windows_amd64.zip
# each with the binary, LICENSE, README.md and the example config. These are
# for users who run their own embedder (e.g. the Docker Compose stack); the
# installers built by packaging/build.sh bundle the embedder.
#   release-build.sh <version> [keep]   (keep: do not clear dist/ first)
set -euo pipefail
VERSION="${1:?usage: release-build.sh <version> [keep]}"
cd "$(dirname "$0")/.."
[ "${2:-}" = keep ] || rm -rf dist
mkdir -p dist
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  os="${target%/*}" arch="${target#*/}"
  name="embedsync_${VERSION}_${os}_${arch}"
  stage="dist/${name}"
  exe=embedsync
  [ "$os" = windows ] && exe=embedsync.exe
  mkdir -p "$stage"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w -buildid= -X github.com/raunaksahahere/postgres-embedding-sync/internal/version.Version=${VERSION}" \
    -o "$stage/$exe" ./cmd/embedsync
  cp LICENSE README.md examples/embedsync.yaml "$stage/"
  if [ "$os" = windows ]; then
    find "$stage" -exec touch -d '2026-01-01 00:00:00Z' {} +
    (cd dist && rm -f "${name}.zip" && TZ=UTC zip -qrX "${name}.zip" "$name")
  else
    tar -C dist --sort=name --mtime='2026-01-01 00:00:00Z' --owner=0 --group=0 --numeric-owner \
      -czf "dist/${name}.tar.gz" "$name"
  fi
  rm -rf "$stage"
done
(cd dist && sha256sum ./* | sed 's# \./# #' | grep -v ' SHA256SUMS$' > SHA256SUMS.tmp && mv SHA256SUMS.tmp SHA256SUMS)
cat dist/SHA256SUMS
