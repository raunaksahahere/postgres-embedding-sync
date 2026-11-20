#!/usr/bin/env bash
# Runs once when the demo database is first initialised (docker-entrypoint-initdb.d).
# Creates least-privilege roles from environment-provided passwords and seeds
# the demo table. `embedsync init` then grants exactly what each role needs.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v repl_pw="$EMBEDSYNC_REPLICATOR_PASSWORD" -v sink_pw="$EMBEDSYNC_SINK_PASSWORD" \
  -v reader_pw="$EMBEDSYNC_READER_PASSWORD" -v db="$POSTGRES_DB" <<'SQL'
CREATE ROLE embedsync_replicator LOGIN REPLICATION PASSWORD :'repl_pw';
CREATE ROLE embedsync_sink LOGIN PASSWORD :'sink_pw';
CREATE ROLE embedsync_reader LOGIN PASSWORD :'reader_pw';
GRANT CONNECT ON DATABASE :"db" TO embedsync_replicator, embedsync_sink, embedsync_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/seed/seed.sql
