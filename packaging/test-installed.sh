#!/usr/bin/env bash
# End-to-end test of an INSTALLED embedsync (from a .deb, .dmg or setup.exe),
# exactly as a developer would use it: setup -> run -> search -> edit -> search
# -> verify -> doctor -> stop.
#
#   test-installed.sh <expected_version> <admin_url> [setup flags...]
#
# Needs `embedsync` and `psql` on PATH and PostgreSQL with pgvector and
# wal_level=logical at <admin_url>. Uses an isolated HOME-like config/cache.
set -euo pipefail
VERSION=$1 ADMIN=$2
shift 2
PY="$(command -v python3 || command -v python)"
ES="$(command -v embedsync)"
echo "== embedsync at $ES"
got="$(embedsync version | tr -d '\r')"
[ "$got" = "embedsync $VERSION" ] || { echo "version: got '$got', want 'embedsync $VERSION'" >&2; exit 1; }

cfgdir="$(mktemp -d)"
export EMBEDSYNC_CONFIG="$cfgdir/embedsync.yaml"
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) win=1; EMBEDSYNC_CONFIG="$(cygpath -w "$EMBEDSYNC_CONFIG")" ;; *) win=0 ;; esac

echo "== doctor before setup (installation only)"
embedsync doctor --skip-embedder

echo "== seed a table"
psql "$ADMIN" -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS public.e2e_docs;
CREATE TABLE public.e2e_docs (id bigint PRIMARY KEY, title text NOT NULL, body text, views int);
INSERT INTO public.e2e_docs VALUES
  (1, 'Replication slots and disk usage', 'An inactive replication slot keeps WAL on disk until its consumer confirms it, which can fill the disk.', 3),
  (2, 'Keeping an indoor cat happy', 'Indoor cats need climbing shelves, daily play with a wand toy and a scratching post.', 7),
  (3, 'Sourdough starter basics', 'Feed the starter equal weights of flour and water every day and keep it warm.', 1);
SQL

echo "== setup $*"
embedsync setup --yes --force --reset-role-passwords --admin-url "$ADMIN" --table public.e2e_docs "$@"

echo "== run"
log="$cfgdir/run.log"
embedsync run >"$log" 2>&1 &
run_pid=$!
stop_run() {
  if [ "$win" = 1 ]; then taskkill //F //T //PID "$(cat /proc/$run_pid/winpid 2>/dev/null || echo $run_pid)" >/dev/null 2>&1 || true
  else kill -INT "$run_pid" 2>/dev/null || true; fi
  wait "$run_pid" 2>/dev/null || true
}
trap 'stop_run; echo "--- run log"; tail -40 "$log"' EXIT
for i in $(seq 600); do
  curl -sf http://127.0.0.1:9187/readyz >/dev/null && break
  kill -0 "$run_pid" 2>/dev/null || { echo "embedsync run exited" >&2; exit 1; }
  sleep 1
done
curl -sf http://127.0.0.1:9187/readyz >/dev/null || { echo "not ready after 600s" >&2; exit 1; }
echo "ready after ${i}s"
embedsync verify --timeout 5m

top() { embedsync search --json --k 5 "$1" | "$PY" -c 'import json,sys; r=json.load(sys.stdin); print(" ".join(h["key"] for h in r["hits"]))'; }
keys="$(top 'how do I entertain my indoor cat')"
echo "cat query -> $keys"
[ "${keys%% *}" = 2 ] || { echo "expected row 2 first" >&2; exit 1; }

echo "== edit and delete"
psql "$ADMIN" -v ON_ERROR_STOP=1 -q -c "UPDATE public.e2e_docs SET body = 'Proof the dough overnight in the fridge for a tangy loaf.' WHERE id = 3" \
  -c "DELETE FROM public.e2e_docs WHERE id = 1"
ok=0
for i in $(seq 60); do
  a="$(top 'proof bread dough overnight in the fridge')"; b="$(top 'replication slot filling the disk with WAL')"
  if [ "${a%% *}" = 3 ] && ! grep -qw 1 <<<"$b"; then ok=1; break; fi
  sleep 0.5
done
[ "$ok" = 1 ] || { echo "edit/delete not searchable: '$a' / '$b'" >&2; exit 1; }
echo "edit + delete searchable"
embedsync verify --timeout 5m
embedsync status
embedsync doctor

echo "== stop"
stop_run
trap - EXIT
sleep 5
if [ "$win" = 1 ]; then
  tasklist | grep -i embedsync-embedder && { echo "embedder still running after stop" >&2; exit 1; } || true
else
  pgrep -f embedsync-embedder && { echo "embedder still running after stop" >&2; exit 1; } || true
fi
echo "INSTALLED E2E OK"
