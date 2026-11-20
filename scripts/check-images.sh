#!/usr/bin/env bash
# Fail if the quickstart depends on a mutable or unpublished image: every image
# that Compose pulls must be pinned by digest, and nothing may use :latest.
# Services with a `build:` section are built from this repository.
set -euo pipefail
cd "$(dirname "$0")/.."
cfg="$(docker compose --profile observability config --format json)"
fail=0
while IFS='|' read -r svc image build; do
  if [ "$build" = "true" ]; then
    echo "ok   $svc: built from source"
    continue
  fi
  case "$image" in
    *:latest|*:latest@*|"") echo "FAIL $svc: '$image' is mutable or missing" >&2; fail=1 ;;
    *@sha256:*) echo "ok   $svc: $image" ;;
    *) echo "FAIL $svc: '$image' is not pinned by digest" >&2; fail=1 ;;
  esac
done < <(python3 -c '
import json, sys
c = json.load(sys.stdin)
for name, s in sorted(c["services"].items()):
    print(name, s.get("image", ""), "true" if "build" in s else "false", sep="|")
' <<<"$cfg")
for f in Dockerfile embedder/Dockerfile; do
  grep -E '^FROM ' "$f" | grep -v '@sha256:' && { echo "FAIL $f: base image not pinned by digest" >&2; fail=1; }
done
exit "$fail"
