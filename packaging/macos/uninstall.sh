#!/bin/sh
# Remove embedsync installed by the .pkg (your config in
# ~/Library/Application Support/embedsync is kept).
set -e
if [ "$(id -u)" != 0 ]; then echo "run with sudo: sudo $0"; exit 1; fi
rm -f /usr/local/bin/embedsync
rm -rf /usr/local/embedsync
pkgutil --forget io.github.raunaksahahere.embedsync >/dev/null 2>&1 || true
echo "embedsync removed"
