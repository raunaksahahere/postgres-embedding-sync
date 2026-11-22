#!/usr/bin/env bash
# Run the LangChain and LlamaIndex examples against a running embedsync.
#   examples/run-integrations.sh URL QUERY [MODE]
# Exits non-zero when either example fails or returns no results.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 URL QUERY [vector|hybrid|text]" >&2
  exit 2
fi
url=$1
query=$2
mode=${3:-hybrid}

cd "$(dirname "$0")/.."

status=0
for ex in langchain llamaindex; do
  echo "== $ex ($mode) =="
  if ! uv run --frozen --project "examples/$ex" python "examples/$ex/example.py" \
      --url "$url" --query "$query" --mode "$mode"; then
    echo "FAIL: $ex" >&2
    status=1
  fi
done
exit $status
