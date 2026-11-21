# Changelog

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
