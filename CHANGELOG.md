# Changelog

## v0.3.0

Several tables in one process, and composite primary keys.

* **Pipelines**: `pipelines:` lists several tables, each with its own key columns,
  allowlisted fields and transform. They share one publication, one replication slot,
  one embedder and every generation. A source transaction that touches several tables
  is applied in one sink transaction, also when its embeddings finish out of order.
  The single-table configuration is unchanged and becomes the pipeline `default`.
* **Composite keys**: `key: [tenant_id, id]`. Keys are typed per column (int, text,
  uuid); backfill windows, range deletes and the planner order them like PostgreSQL's
  row comparison. Documents are identified by `(pipeline, key)`, so ids may repeat
  across tables.
* Search hits, `status`, `verify`, the DLQ and backfill jobs report the pipeline;
  `embedsync backfill --pipeline P` re-scans one table. With several pipelines the
  migration gate judges hits as `<pipeline>/<key>` in `eval_qrels`.
* `embedsync init` (and `setup`) add new pipelines' tables to the publication and grant
  their columns on every run. They never remove a table: one that no pipeline names is
  reported, with the statement to drop it by hand if it was never synced.
* A generation is ready, and a migration gates it, only after every pipeline's backfill
  is done; a resumed migration re-checks this.
* **Upgrade**: stop `embedsync run`, install the new version, run
  `embedsync init --admin-url <admin URL>` (new flag; or the admin DSN variables), then
  start `run`. It applies `sink/0002_pipelines.sql` (see docs/install.md and
  docs/operations.md). Existing documents are not re-embedded and keep serving. Until
  then every command, and `embedsync doctor`, says the sink needs `embedsync init`.
  Downgrading after `init` is not supported.

## v0.2.0

Install embedsync from a native installer and run it without cloning the repository,
installing Python or running Docker.

* **Installers** for Windows (`setup.exe`, x64), macOS (`.dmg` with a `.pkg`,
  Apple silicon) and Debian/Ubuntu (`.deb`, amd64 and arm64). Each bundles the
  embedding service. Three variants:
  * `minilm`: all-MiniLM-L6-v2 included
  * `nomodel`: setup downloads any Hugging Face model you choose
  * `full`: all three evaluated models included
* **`embedsync setup`**: guided first run. It checks PostgreSQL (version, `wal_level`,
  pgvector) and explains how to install or fix it. You pick the table, columns and
  model. It creates least-privilege roles with random passwords and writes the config
  plus a private credentials file. `--yes` runs it non-interactively.
* **`embedsync doctor`**: checks the installation, config, database and embedder, with fixes.
* **`embedsync model list|add`**: add any Hugging Face model, pinned to a commit, then
  `embedsync migrate --to` it.
* **Managed embedder** (`embedder.managed: true`): embedsync starts the bundled embedder
  on a private localhost port, shares it between `run` and `search`/`migrate`, and
  stops it on exit, including when embedsync is killed.
* Config is found automatically (`$EMBEDSYNC_CONFIG`, `./embedsync.yaml`, then the
  per-user file written by setup). An `embedsync.env` next to it supplies credentials
  that are not already set in the environment.
* CLI-only archives now include Windows (`.zip`).
* Release tags now use the standard `vX.Y.Z` form. The previous release was tagged `v.0.1.0`.

CI builds every installer, installs it on clean systems (Debian 12, Ubuntu 22.04 and
24.04, macOS 15, Windows Server 2025) and runs setup → run → search → edit → verify →
doctor → uninstall against a real PostgreSQL with pgvector.

## v0.1.0

First release: CDC pipeline with a transactional checkpoint, watermark backfill, DLQ,
gated blue/green model migrations, CPU embedder, Compose quickstart, benchmarks.
