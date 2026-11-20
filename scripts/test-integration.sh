#!/usr/bin/env bash
# Run the integration suite against a throwaway pgvector container.
#   scripts/test-integration.sh 17            # PostgreSQL 17
#   RACE=1 scripts/test-integration.sh 16     # binary under test built with -race
#   RUN=TestStreamCorrectness scripts/test-integration.sh 18
set -euo pipefail
MAJOR="${1:-17}"
PORT="${PORT:-$((55400 + MAJOR))}"
NAME="embedsync-it-pg${MAJOR}"
case "$MAJOR" in
  16) DIGEST=sha256:484e838076e04939d27d3e176e91c86eea96236126023be8d3059761122e579f ;;
  17) DIGEST=sha256:b513c48566e4777cd3370451400000c78eda6a08131d68f4c31d4a76f27bd8fd ;;
  18) DIGEST=sha256:38681dc6eaf55308e2a4c6de392d131652dcfac70e1263e35cd9b5ea3fe39782 ;;
  *) echo "unsupported PostgreSQL major $MAJOR (16, 17, 18)" >&2; exit 2 ;;
esac
IMAGE="pgvector/pgvector:0.8.7-pg${MAJOR}-bookworm@${DIGEST}"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=it -p "127.0.0.1:${PORT}:5432" "$IMAGE" \
  -c wal_level=logical -c max_replication_slots=64 -c max_wal_senders=64 -c max_connections=400 \
  -c max_slot_wal_keep_size=4GB >/dev/null
[ "${KEEP_CONTAINER:-0}" = 1 ] || trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 60); do
  docker exec "$NAME" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
sleep 1
docker exec "$NAME" psql -U postgres -tAc "select version()"
export EMBEDSYNC_IT_ADMIN_DSN="postgres://postgres:it@127.0.0.1:${PORT}/postgres?sslmode=disable"
export EMBEDSYNC_IT_RACE="${RACE:-0}"
go test -tags integration -count="${COUNT:-1}" -timeout 40m ${RUN:+-run "$RUN"} -v ./test/integration/ 2>&1 | tee "${LOG:-/dev/null}"
