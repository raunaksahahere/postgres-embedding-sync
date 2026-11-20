#!/usr/bin/env bash
# Quickstart end to end, exactly as the README shows it: start the Compose
# stack, wait for the initial backfill, edit a row and delete a row, search,
# verify the whole sink, check metrics and produce a gate report.
# Writes e2e-results.json. Uses its own Compose project so it never touches the
# quickstart's data (set E2E_PROJECT to override; E2E_KEEP=1 keeps the stack).
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="${E2E_PROJECT:-embedsync-e2e}"
export PG_PORT="${PG_PORT:-56532}" EMBEDDER_PORT="${EMBEDDER_PORT:-58200}" EMBEDSYNC_PORT="${EMBEDSYNC_PORT:-59387}"
dc() { docker compose "$@"; }
es() { dc exec -T embedsync embedsync "$@"; }
psql_app() { dc exec -T postgres psql -U postgres -d app -XAtq -c "$1"; }
cleanup() {
  if [ "${E2E_KEEP:-0}" != 1 ]; then dc down --remove-orphans >/dev/null 2>&1 || true; docker volume rm "${COMPOSE_PROJECT_NAME}_pgdata" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
fail() { echo "E2E FAIL: $*" >&2; dc logs --tail 80 embedsync init >&2 || true; exit 1; }

start=$(date +%s)
dc down --remove-orphans >/dev/null 2>&1 || true
docker volume rm "${COMPOSE_PROJECT_NAME}_pgdata" >/dev/null 2>&1 || true
dc up -d --build
echo "waiting for the initial backfill (first run downloads the models)..."
for i in $(seq 1 600); do
  if curl -fsS "http://127.0.0.1:${EMBEDSYNC_PORT}/readyz" >/dev/null 2>&1 && out=$(es status --json 2>/dev/null) && python3 -c '
import json,sys
s=json.loads(sys.stdin.read())
gens=[g for g in s.get("generations") or [] if g.get("state") in ("ready","active")]
jobs=s.get("backfill_jobs") or []
sys.exit(0 if gens and jobs and all(j.get("State") in ("done","cancelled") for j in jobs) else 1)' <<<"$out"; then break; fi
  [ "$i" = 600 ] && fail "stack not ready after 20 minutes"
  sleep 2
done
ready=$(date +%s)

top() { es search --json --k 3 "$1" | python3 -c 'import json,sys; print(" ".join(h["key"] for h in json.load(sys.stdin)["hits"]))'; }
before_delete=$(top "replication slot filling the disk with WAL")
[ "${before_delete%% *}" = 1 ] || fail "expected row 1 first before delete, got: $before_delete"

psql_app "UPDATE documents SET title = 'Keeping an indoor cat happy', body = 'Indoor cats need climbing shelves, daily play with a wand toy and a scratching post near the window.' WHERE id = 2"
psql_app "DELETE FROM documents WHERE id = 1"
t_write=$(date +%s.%N)
for i in $(seq 1 100); do
  after_edit=$(top "how do I entertain my indoor cat")
  after_delete=$(top "replication slot filling the disk with WAL")
  if [ "${after_edit%% *}" = 2 ] && [[ " $after_delete " != *" 1 "* ]]; then break; fi
  [ "$i" = 100 ] && fail "edit/delete not searchable: edit=$after_edit delete=$after_delete"
  sleep 0.2
done
t_visible=$(date +%s.%N)

verify=$(es verify --json --timeout 120s) || fail "verify reported differences: $verify"
metrics=$(curl -fsS "http://127.0.0.1:${EMBEDSYNC_PORT}/metrics")
grep -q '^embedsync_consumer_state{state="idle"} 1' <<<"$metrics" || grep -q '^embedsync_consumer_state{state="active"} 1' <<<"$metrics" || fail "consumer not idle/active"
grep -q 'Indoor cats' <<<"$metrics" && fail "row text leaked into metrics"
gate=$(es migrate --to bge --gate-only --json 2>/dev/null || true)
python3 -c 'import json,sys; r=json.loads(sys.stdin.read()); assert "gates" in r and isinstance(r["pass"], bool), r' <<<"$gate" || fail "no gate report"

python3 - "$start" "$ready" "$t_write" "$t_visible" <<EOF
import json, sys
start, ready, tw, tv = map(float, sys.argv[1:])
verify = json.loads('''$verify''')
gate = json.loads('''$gate''')
out = {
  "ok": True,
  "stack_ready_s": round(ready - start, 1),
  "edit_and_delete_searchable_within_s": round(tv - tw, 2),
  "search_before_delete": "$before_delete".split(),
  "search_after_edit": "$after_edit".split(),
  "search_after_delete": "$after_delete".split(),
  "verify": verify,
  "gate_only_report": {"from": gate["from"], "to": gate["to"], "pass": gate["pass"],
                       "gates": [{k: g[k] for k in ("name", "observed", "threshold", "pass")} for g in gate["gates"]],
                       "note": "CI fixture: 24 synthetic queries; gate-only (no promotion)"},
}
json.dump(out, open("e2e-results.json", "w"), indent=2)
print(json.dumps({k: out[k] for k in ("stack_ready_s", "edit_and_delete_searchable_within_s", "search_after_edit", "search_after_delete")}))
EOF
echo "E2E OK"
