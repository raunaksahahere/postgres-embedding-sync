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
* `identity_mismatch`, `slot_lost`, `slot_ahead` — see *Slot loss* below.
* `startup_check` — catalog or configuration problem found at start; the detail names it.

## Quarantine (DLQ)

```
embedsync dlq list                  # open records: generation, key, source LSN, reason, attempts
embedsync dlq inspect --id 12
embedsync dlq replay [--id 12]      # retries; skips records superseded by newer edits/deletes
```

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

## PostgreSQL images

The Compose file and CI pin `pgvector/pgvector:0.8.7-pg{16,17,18}-bookworm` by digest:

| major | digest |
|---|---|
| 16 | `sha256:484e838076e04939d27d3e176e91c86eea96236126023be8d3059761122e579f` |
| 17 | `sha256:b513c48566e4777cd3370451400000c78eda6a08131d68f4c31d4a76f27bd8fd` (default) |
| 18 | `sha256:38681dc6eaf55308e2a4c6de392d131652dcfac70e1263e35cd9b5ea3fe39782` |

`PG_IMAGE=pgvector/pgvector:0.8.7-pg16-bookworm@sha256:484e… docker compose up -d` runs the
demo on another major.
