# Operations runbook

## Roles and credentials

`embedsync init` (run once with admin DSNs) creates the control schema, the marker
table and the publication on the source, and the sink schema. Three roles are expected
(the Compose demo creates them in `deploy/postgres/10-roles-and-seed.sh`):

| role | privileges |
|---|---|
| `embedsync_replicator` | `REPLICATION`, column-level `SELECT` on the key + allowlisted columns, `INSERT/DELETE` on `embedsync_control.markers` |
| `embedsync_sink` | owns the sink schema (`embedsync`) |
| `embedsync_reader` | `SELECT` on the serving catalog and chunk tables only (not `source_state`, not the DLQ) |

DSNs come only from environment variables named in the config (`*_env`). Logs and
metrics never contain row text, keys of quarantined rows are visible only through
`embedsync dlq` (sink role).

PostgreSQL settings on the source: `wal_level=logical`, a free replication slot and WAL
sender, and **`max_slot_wal_keep_size`** set to a value your disk can hold (the demo uses
2 GB). Without it, a stopped consumer can fill the primary's disk.

## Retained WAL

A logical slot pins all WAL after its `restart_lsn`, not only WAL of the published
table. embedsync releases it as soon as changes are durable in the sink, and on a
quiet table through idle checkpoints (keepalive positions). Retained WAL grows when:

* the embedding service is down or slow (`embedsync_consumer_state` = active with a
  growing `embedsync_freshness_lag_seconds`, `embedsync_retries_total{component="embedder"}` rising);
* the sink is unavailable (`component="sink"` retries);
* the pipeline is **paused** (schema change, poison row with `on_poison: pause`);
* `embedsync run` is not running (`embedsync_slot_active` = 0, or no metrics at all).

Watch `embedsync_slot_retained_wal_bytes`, `embedsync_slot_safe_wal_bytes`
(`safe_wal_size`: bytes left before the slot is invalidated by
`max_slot_wal_keep_size`; −1 when unlimited) and `embedsync_slot_wal_status`.

### Alerts (`deploy/prometheus/alerts.yml`)

| alert | meaning | first action |
|---|---|---|
| `EmbedsyncFreshnessSLO` / `EmbedsyncFreshnessP99` | oldest unapplied commit (or p99) older than `alerts.freshness_seconds` | check embedder health/throughput and sink latency (`embedsync_sink_transaction_seconds`) |
| `EmbedsyncConsumerStalled` | pending work but the checkpoint has not moved for `stall_after` | logs; `embedsync status`; restart if wedged |
| `EmbedsyncPaused` | deliberate stop before acknowledging | `embedsync status` shows the reason; see below |
| `EmbedsyncRetainedWALWarning` / `Critical` | slot pins more than the configured bytes | fix the cause; if it cannot be fixed before the disk fills, plan a rebuild |
| `EmbedsyncSlotNearInvalidation` | under 256 MiB before `max_slot_wal_keep_size` invalidates the slot | same, urgently |
| `EmbedsyncSlotLost` | the slot was invalidated (`wal_status = lost`) | rebuild (below) |
| `EmbedsyncUnresolvedRows` | quarantined rows for 10 min | `embedsync dlq list` / `inspect` / `replay` |

Disk alarms on the primary are still needed: `max_slot_wal_keep_size` protects the
primary by sacrificing the slot, and other slots or archiving failures also retain WAL.

## Pauses

All pipelines share one stream, so a pause stops every table; the pause detail and
`embedsync status` name the pipeline that caused it, and `--accept-schema-change` accepts
only that pipeline's change.

```
embedsync status              # reason and detail
```

* `schema_incompatible` — an allowlisted column was renamed/dropped/retyped or the key
  changed, and the **stream** reached that change. Nothing after the last checkpoint was
  acknowledged. Update `source.fields` (and re-run `embedsync init` if the publication
  column list must change), then `embedsync run --accept-schema-change`. The new shape
  is accepted only while the checkpoint is still at the recorded pause point, so it is
  applied from exactly the first changed transaction. If the catalog changed but the
  consumer is behind, keep the *previous* mapping: embedsync streams with it up to the
  change and pauses there (changing the mapping early is refused with a message saying
  so). Changing the *transform* or field names changes fingerprints, so affected rows
  are re-embedded as they stream through; queue `embedsync backfill` to re-render
  everything.
* `poison_row` — with `on_poison: pause`, a row the embedder rejects. Fix the row in the
  source (or the embedder) and restart; or switch to `quarantine`.
* `unsupported_input` — e.g. an update did not include a large (TOASTed) column and its
  previous value is unknown because the row was never seen and no backfill can cover
  it. Run `embedsync backfill`, or set `REPLICA IDENTITY FULL` on the table.
* `transaction_too_large` — one source transaction exceeds
  `pipeline.max_transaction_changes`; raise it and restart.
* `fanout_limit` — one source transaction changes more documents of a view than its
  `max_fanout_per_txn` (for example a tag renamed on every document). The transaction
  was not acknowledged and nothing of it was applied; the detail names the view and the
  transaction's LSN. Raise `max_fanout_per_txn` in the view and restart `run`: the
  transaction is then applied whole. The limit is not part of the view fingerprint,
  so changing it re-embeds nothing.
* `identity_mismatch`, `slot_lost`, `slot_ahead` — see *Slot loss* below.
* `startup_check` — catalog or configuration problem found at start; the detail names it.

## Quarantine (DLQ)

```
embedsync dlq list                  # open records: generation, key, source LSN, reason, attempts
embedsync dlq inspect --id 12
embedsync dlq replay [--id 12]      # retries; skips records superseded by newer edits/deletes
```

A view document is quarantined as a whole, keyed by its root key. Reasons specific to
views: `too_many_children` (more rows than `max_children` in one child or link; raise
the limit and `dlq replay`), plus the usual render error, `document_too_large` and
embedder rejections. Replay assembles the document again from the sink's state.

Quarantined rows have no chunks in that generation (search never serves an outdated
version), count as unresolved in `status`, `/metrics` and `verify`, and block migration
promotion while `require_zero_unresolved` is set.

## Slot loss, restore from backup, failover

On start, embedsync compares the stored system identifier, database, slot,
publication, table and key type with the live source, and refuses if the slot is
missing, `lost`, or its `confirmed_flush_lsn` is ahead of the checkpoint (which
would mean changes were acknowledged that the sink never applied). Recovery:

```
embedsync rebuild --confirm   # drops the slot (if any) and the checkpoint, marks generations stale,
                              # creates a fresh generation <model>_rN
embedsync run                 # creates a new slot, backfills the new generation from scratch
```

The previous serving generation keeps answering searches (stale) until the rebuilt
generation is ready and promoted (`embedsync migrate --to <model>_rN`). Logical slots
are not carried across a physical failover before PostgreSQL 17's failover slots; after
a failover without synced slots, treat it as slot loss.

## Upgrades

* **embedsync**: stop (`SIGTERM`, graceful: in-flight units finish or roll back), replace
  the binary or image, start. Sink schema migrations are versioned and applied by
  `embedsync init`; run it before starting a version whose release notes say so. Every
  command checks the sink schema version and refuses (with "run `embedsync init`") to
  work against an older schema.
* **From a single-table release (v0.2.0 and earlier) to pipelines**: stop `run`, replace
  the binary, run `embedsync init` with the admin DSNs or `--admin-url` (it applies
  `sink/0002_pipelines.sql`: existing rows of `source_state`, every chunk table, `dlq` and
  `backfill_jobs` become the pipeline `default`, and the accepted relation moves to
  `pipeline_relations`), then start `run` with the unchanged config. Nothing is
  re-embedded; checkpoint, generations, rollback window and DLQ records carry over. The
  migration rebuilds each chunk table's primary key, which takes a lock for a time
  proportional to the table; searches wait meanwhile. Downgrading after `init` is not
  supported (the old binary refuses to start on the upgraded checkpoint).
* **From v0.3.0 to views**: stop `run`, install the new version, run
  `embedsync init --admin-url <admin URL>` (or the admin DSN variables), then start
  `run`. `init` applies `sink/0003_views.sql`: it adds `source_refs` and the backfill job
  kind, and changes nothing else. Nothing is re-embedded and existing documents keep
  serving. Until `init` runs, every command says the sink needs it. Downgrading after
  `init` is not supported.
* **Adding a view** (example: [`examples/embedsync-views.yaml`](../examples/embedsync-views.yaml)):
  every table of the view needs the tenant column first in its primary key (and in its
  references). Add the `views:` entry, run `embedsync init` (grants columns and adds
  the tables to the publication) and restart `run`. embedsync queues one `state` job per
  table, then one `render` job per live generation; documents appear once all state
  jobs are done. A table can belong to only one pipeline or view, so a table already
  synced as a pipeline cannot be reused. `embedsync status` lists the view (kind
  `view`) and its tables (kind `view_table`, named `<view>:<table>`) and the jobs.
  `embedsync backfill --pipeline <view>` re-scans all of its tables and re-renders.
* **Changing a view**: the view fingerprint covers tables, keys, references, order,
  fields (including physical column names) and the transform, but not `max_children` or
  `max_fanout_per_txn`. Renaming a column of a view's table in the config, like any
  other change to these, is a new generation (`embedsync migrate`); a pipeline's field
  column is not part of its fingerprint, a view's is. Changing the two limits needs
  only a restart.
* **Adding a table** (example: [`examples/embedsync-multi.yaml`](../examples/embedsync-multi.yaml)):
  move the existing table under `pipelines:` with the name `default`
  (any other name would be a new, empty pipeline), add the new pipeline, run
  `embedsync init` (grants its columns to the replicator role and adds it to the
  publication with its column list) and restart `run`. The new pipeline is registered,
  and one backfill job per live generation fills it while streaming continues;
  `embedsync status` lists pipelines and jobs. A migration candidate must cover every
  configured pipeline before its gates run.
* **Changing or removing a pipeline**: its table and key cannot change in place
  (startup pauses with `identity_mismatch`); a new transform needs a new generation
  (`embedsync migrate`), as before. Removing a pipeline from the config pauses startup
  (`startup_check`); restore it or set up a new sink schema. `embedsync init` never
  removes a table from the publication; a table that no pipeline names makes `run` stop
  until it is restored or, if it was never synced, dropped from the publication by hand.
* **PostgreSQL major upgrade** of the source: `pg_upgrade` does not keep logical slots
  before PG 17 (17+ migrates them if the subscriber/consumer is caught up). Stop writers,
  wait for `embedsync status` to show idle, upgrade, then either reuse the migrated slot
  (17→18) or run `rebuild --confirm`.
* **Embedding model / revision**: never edit a generation's model in place; add a model
  key and run `embedsync migrate --to <key>`. Revisions are commit SHAs, so a model
  repository update cannot silently change vectors.

* **From v0.4.0 to providers** (v0.5.0): stop `run`, install the new version, run
  `embedsync init --admin-url <admin URL>` (or the admin DSN variables), then start `run`.
  `init` applies `sink/0004_providers.sql`: it adds `generations.provider` (default
  `{"name": "local", "kind": "local"}`) and changes nothing else. Existing generations are
  recorded as built by the built-in embedder and keep serving; nothing is re-embedded. Until
  `init` runs, every command says the sink needs it. Downgrading after `init` is not
  supported.
* **From v0.5.0 to search** (v0.6.1): stop `run`, install the new version, run
  `embedsync init --admin-url <admin URL>`, then start `run`. `init` applies
  `sink/0005_search.sql` (adds the `meta` column and its GIN index to every chunk table, and
  two generation columns). Nothing is re-embedded. Then `run` builds the full-text index and
  fills declared metadata in the background. Filtered, text and hybrid requests answer
  `503 search_index_building` until `embedsync status` shows `search=ready` for the serving
  generation. Vector requests without a filter keep working. Details: [search](search.md#upgrading-an-existing-installation).

## Search

* **Search builder.** `run` completes the search columns of every live generation that lacks
  them for the current configuration: after the upgrade, and after a change of a `metadata:`
  declaration or of `search.text_config`. Full text is an expression index on
  `to_tsvector(<text_config>, content)`, built with `CREATE INDEX CONCURRENTLY` (writes go on;
  no chunk row is rewritten, so the vector index is untouched). Pipelines and views that
  declare metadata are re-scanned from the source with their stored vectors, so nothing is
  re-embedded; their chunk rows are rewritten with the metadata (new row versions, so plan a
  `VACUUM` afterwards on large tables). The log line
  `search columns ready: filtered, text and hybrid search enabled` marks the end.
* **Readiness.** `embedsync status` prints `search=building` or `search=ready` on each
  generation line (`generations[].search` in `--json`). Until a generation is ready,
  filtered, text and hybrid requests to it get `503 search_index_building` with
  `Retry-After: 30`. Vector search without a filter or tenant is not affected.
* **Verification.** `embedsync verify` reports `meta_stale` per generation and pipeline and
  `fts_index_missing` per generation. While the builder runs it reports `search columns:
  building (metadata and full-text index not checked)`.
* **Tenant trust boundary.** With `search.tenant_filter`, the tenant comes from a request
  header that embedsync trusts as given. embedsync does not authenticate callers, and
  `/search` is served on the metrics listener (`metrics.listen`, default `:9187`) without
  authentication. Expose that listener only to a gateway that authenticates the caller and
  overwrites the tenant header on every request. Requests without the header are refused
  (`400 tenant_required`).
* **Snapshot pinning.** Each request reads one generation in one repeatable-read
  transaction. A promotion or rollback during a request is seen as a whole: the query is
  re-embedded with the newly serving encoder, and every branch of a hybrid request reads the
  same snapshot. A request that names a `generation` never changes generation.
* **pgvector version.** Filtered vector search needs pgvector 0.8 or later (iterative index
  scan). On an older version, a selective filter can return fewer than `k` hits. `exact: true`
  is the workaround for a single request.

## Models: download, cache, offline

The embedder downloads each `name@revision` in `EMBEDDER_MODELS` from Hugging Face on
first start into `HF_HOME=/models` (Compose volume `hf-cache`, about 290 MB for the three
demo models) and refuses to serve any other model or revision. Afterwards it runs
offline with `HF_HUB_OFFLINE=1`:

```
HF_HUB_OFFLINE=1 docker compose up -d
```

To pre-seed an air-gapped host, copy the populated volume (or a host directory mounted at
`/models`) from a machine that ran it once.

## Embedding providers

### Configure a provider

1. Put the API key in the environment, or in `embedsync.env` next to the config (written
   by `embedsync setup`, mode 0600). Name only the variable in the config (`api_key_env`).
2. Add the `providers:` entry and a model with `provider:`. `dimension` is the size the
   model returns, and `request_dimensions` (optional) must equal it.
3. Run `embedsync doctor`. It checks that each used provider's key variable is set and
   prints its kind and estimated price. It does not call the endpoint.
4. Run `embedsync migrate --to <model>` and read the gate report. A new generation serves
   only after it passes.

### Switch a model to a provider

A generation keeps the provider and settings it was built with, so a model key is never
moved to another provider in place. Add a new key (for example `gemini768`), then
`embedsync migrate --to gemini768`. The report names the active and candidate providers.
`embedsync migrate rollback` returns searches to the previous generation and its provider.
`Set` refuses to serve a generation whose provider was removed or changed kind, so
restore the old definition before rolling back to it.

### Metrics to watch

| metric | what it tells you |
|---|---|
| `embedsync_provider_retries_total{provider,reason}` | `throttled` (429), `unavailable` (outage, 5xx, connection), `invalid` (malformed response). Sustained `throttled` means the configured limits are above the quota. |
| `embedsync_provider_retry_wait_seconds_total{provider}` | time spent waiting before retries, including `Retry-After` |
| `embedsync_provider_requests_total{provider,outcome}` and `embedsync_provider_request_seconds` | request outcomes and latency per provider |
| `embedsync_provider_inputs_total{provider}` | inputs embedded per provider |
| `embedsync_provider_tokens_total{provider,source}` | tokens; `source="estimated"` means the provider reported no usage |
| `embedsync_provider_estimated_cost_usd_total{provider,pricing_source}` | an estimate: tokens times the configured price. Check spend against the provider's own billing. |

The freshness alerts in `deploy/prometheus/alerts.yml` apply to providers as they do to the
built-in embedder.

### GPU embeddings with Ollama on AMD (ROCm)

The measured GPU run used Ollama's ROCm image (`ollama/ollama:rocm`) on an AMD Radeon RX 6500
(4 GB, Navi 24). That card is not in ROCm's supported list, so the container needs
`HSA_OVERRIDE_GFX_VERSION=10.3.0`. With it, all 7 of 7 layers of `all-minilm` were offloaded to
the GPU, and the OpenAI-compatible `/v1/embeddings` endpoint reported token usage.
Point a provider at it with `kind: openai` and `url: http://127.0.0.1:11434/v1`, and set the
model's `dimension: 384`. Pin `revision` to the Ollama model digest. Other AMD cards, CUDA GPUs
and NVIDIA's catalog were not verified.

## PostgreSQL images

The Compose file and CI pin `pgvector/pgvector:0.8.7-pg{16,17,18}-bookworm` by digest:

| major | digest |
|---|---|
| 16 | `sha256:484e838076e04939d27d3e176e91c86eea96236126023be8d3059761122e579f` |
| 17 | `sha256:b513c48566e4777cd3370451400000c78eda6a08131d68f4c31d4a76f27bd8fd` (default) |
| 18 | `sha256:38681dc6eaf55308e2a4c6de392d131652dcfac70e1263e35cd9b5ea3fe39782` |

`PG_IMAGE=pgvector/pgvector:0.8.7-pg16-bookworm@sha256:484e… docker compose up -d` runs the
demo on another major.
