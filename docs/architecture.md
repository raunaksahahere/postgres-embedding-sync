# Architecture

embedsync is one Go process (`embedsync run`) plus a CPU embedding service. It reads
one or more PostgreSQL tables (*pipelines*) through one publication and one logical
replication slot and maintains one pgvector chunk table per *generation* (embedding
model) in a sink schema, which can be in the same database or another one. Every
generation covers every pipeline; a document is identified by `(pipeline, key)`.

```
 source PostgreSQL                                   sink PostgreSQL (+pgvector)
 ┌──────────────────────┐   pgoutput (proto 1)      ┌──────────────────────────────────┐
 │ pipelines' tables    │──┐                        │ embedsync.checkpoints            │
 │ embedsync_control.   │  │  ┌────────┐ ┌───────┐  │ embedsync.source_state, relations│
 │   markers            │──┴─▶ reader ├─▶planner│  │ embedsync.generations / serving  │
 │ publication          │     └────────┘ └───┬───┘  │ embedsync.chunks_<generation>    │
 │  (key + allowlisted  │      ▲ standby     │units │ embedsync.dlq / backfill_jobs    │
 │   columns, markers)  │      │ feedback   ┌▼────┐ │ embedsync.migrations             │
 └──────────┬───────────┘      │ = durable  │embed│ └──────────────▲───────────────────┘
            │ bounded PK scans  │ LSN       └┬────┘                │ one transaction per unit:
       ┌────▼─────┐             │      ┌─────▼────┐   effects + DLQ + backfill progress
       │ backfill │─ registry ──┼─────▶│committer ├──────────────────┘ + checkpoint LSN
       └──────────┘             └──────┴──────────┘
```

## Data path

1. **Reader** (`internal/pipeline/reader.go`) runs `START_REPLICATION … (proto_version '1',
   publication_names '…')`. It assembles whole transactions (Begin…Commit), checks every
   Relation message against the accepted column signature, and decodes marker rows. It
   never acknowledges anything itself: every standby status update reports the
   *durable* LSN (the checkpoint committed in the sink) as write, flush and apply
   position, including replies to keepalives.
2. **Planner** (`planner.go`) groups transactions into *units* (bounded by
   `max_batch_txns` / `max_batch_docs`), coalescing changes per key: the last version
   of a row in a unit wins. It resolves unchanged-TOAST columns (`'u'` in pgoutput)
   from `source_state`, from uncommitted earlier units, or by deferring to an
   in-progress backfill; if none applies it pauses rather than guess.
3. **Embed stage** (`embed.go`) renders the allowlisted fields through the template,
   splits into word windows, and computes a fingerprint per chunk:
   `sha256(transform config ‖ model name ‖ model revision ‖ document prompt ‖ text)`.
   A chunk whose fingerprint is already stored (or in an LRU cache) is not re-embedded.
   Requests go through a token-bucket rate limiter and bounded concurrency; transient
   errors retry with capped full-jitter backoff; a `422` with rejected indexes marks
   those inputs as poison.
4. **Committer** (`committer.go`) applies units strictly in order. One sink
   transaction contains: `source_state` changes, per-generation chunk upserts/deletes
   (with obsolete trailing chunks trimmed), quarantine records, generation and backfill
   progress, and the checkpoint `UPDATE … SET lsn = <unit end LSN>`. Only after commit
   does the durable LSN advance. A unit whose end LSN is ≤ the stored checkpoint is
   skipped, so redelivery after a crash is idempotent.

Delivery from PostgreSQL is at-least-once (anything after the slot's confirmed position
is resent after a restart); effects are effectively-once because they and the position
commit atomically and replays below the checkpoint are dropped.

## Pipelines and keys

A pipeline is one source table with its key columns, allowlisted fields and transform
(`pipelines:` in the config; the single-table `source.table` form is the pipeline
`default`). All pipelines share the reader, planner, embed stage and committer, so the
ordering argument above covers them together: a unit contains whole source
transactions whatever tables they touch, and it commits only after every embedding it
needs has returned, in commit-LSN order. A transaction that writes two tables is
therefore visible in the sink all at once or not at all.

Keys are carried as text. `keys.Codec` encodes a one-column key as its value (the
format of every installation before pipelines) and a composite key as a JSON array of
its column values, and compares encoded keys column by column: integers numerically,
text bytewise (`COLLATE "C"`), uuids as canonical strings. The backfill scan uses the
same order through a row comparison over the typed source columns,
`(t.tenant_id, t.slug COLLATE "C") > ($1::bigint, $2::text COLLATE "C") ORDER BY …`, and
range deletes in the sink decode the encoded column (`(source_key::jsonb->>0)::bigint`)
behind a `CASE` on the pipeline, so one pipeline's casts never touch another's keys.

Per-pipeline sink state: `source_state`, chunk tables, `dlq` and `backfill_jobs` carry a
`pipeline` column; `pipeline_relations` holds each pipeline's accepted table, key columns,
key types and column signature; `generations.pipelines` holds each pipeline's transform
fingerprint. A pipeline added to the config is registered at the next start and
backfilled into every live generation (one job per pipeline and generation).

## Idle vs. stalled

Unpublished WAL (other tables, other databases' activity, checkpoints) does not produce
transactions for this publication, but the slot still pins it. When the pipeline is
fully caught up, keepalives' `ServerWALEnd` becomes an *idle item* that flows through the
committer and advances the checkpoint, so retained WAL is released even on a quiet
table. The consumer state is:

| state | meaning |
|---|---|
| `idle` | nothing pending, connection alive |
| `active` | pending transactions and the durable LSN keeps advancing (possibly behind; see `embedsync_freshness_lag_seconds`) |
| `stalled` | pending work but no durable progress for `stall_after`, or no server message for `stall_after` |
| `paused` | stopped deliberately before acknowledging (schema change, poison row with `on_poison: pause`, startup check) |

## Watermark backfill

Backfill follows the DBLog/Sequin watermark design: for each window the executor
commits a `low` marker, waits until every transaction that was still in progress
right after it has ended (WAL commit order is not visibility order, so a transaction
committed before `low` in the stream could otherwise be invisible to the scan), scans
up to `batch_size` rows by primary key after the last key, hands the rows to the planner through an in-process registry, then commits a
`high` marker. When the stream reaches `high`, the planner drops snapshot rows whose
keys changed between the markers (the stream already carries newer versions), emits
the remaining rows plus a *range delete* for `(last_key, window_end]` that removes sink
rows absent from the snapshot, and records progress — all in the same unit and sink
transaction. Deletes, key changes and truncates are therefore reconciled without
resurrecting rows. A window that observed an unchanged-TOAST value it could not
resolve is discarded and retried with half the batch size. Progress (`last_key`) is
stored with the effects, so a restarted backfill resumes from the last applied window.
A `start` marker gives each new generation an exact stream position from which it
receives changes, before its first window.

## Generations and model migration

A generation = model name + Hugging Face revision + dimension + distance + prompts +
index parameters + its own chunk table and HNSW index. Requests are pinned to a
generation: the query encoder (model, revision, query prompt) is read from the
generation row, so two 384-dimension models can never be mixed even though their
vectors would fit the same column type.

`embedsync migrate --to <model>`:

1. **register** a `pending` generation with its table and index, queue its backfill;
2. **backfill** while the stream keeps maintaining it (`building` → `ready`);
3. **catch-up**: write a barrier marker and wait until the shared checkpoint passes it;
4. **gates**, evaluated at one sink snapshot and persisted in `embedsync.migrations`:
   coverage (every expected chunk present and current, none extra), zero unresolved
   rows, catch-up, candidate ANN-vs-exact recall@10 ≥ threshold, and paired-bootstrap
   non-inferiority: the lower bound of the CI for Δ nDCG@10 and Δ recall@10
   (candidate − active) must be ≥ −margin;
5. **promote** atomically (`serving` row: generation + previous) or **reject**
   (state `rejected`, `retire` marker stops its maintenance).

Both generations stay maintained during the rollback window; `migrate rollback`
swaps back instantly, `migrate finalize` (or expiry of the window) retires the old one.
Promoting again inside the window makes the generation serving at that moment the
rollback target and retires the older one. Retiring a generation cancels its backfill
jobs. `--gate-only` evaluates and records the report but never promotes or rejects.
Every step is persisted, so an interrupted migration resumes.

## Failure handling

| failure | behaviour |
|---|---|
| crash before sink commit | unit rolled back; replayed from checkpoint |
| crash after commit, before feedback | slot resends; units ≤ checkpoint skipped |
| embedding service down / 5xx / timeouts | retries with backoff; nothing acknowledged; freshness alert fires |
| poison input (422) | `on_poison: quarantine` → stale chunks removed and a versioned DLQ record written in the same transaction; `pause` → stop before the row |
| unchanged TOAST without known prior value | defer to backfill or pause |
| incompatible schema change | pause before acknowledging the transaction; restart needs `--accept-schema-change` after updating the mapping |
| slot lost / source restored | startup identity check refuses; `embedsync rebuild --confirm` rebuilds into a fresh generation |
| sink errors | the unit is retried with backoff |

## Code map

| path | role |
|---|---|
| `cmd/embedsync` | CLI |
| `internal/pipeline` | reader, planner, embed stage, committer, backfill |
| `internal/app` | startup checks, supervisor, status, verify, DLQ replay, rebuild |
| `internal/migrate` | generations lifecycle, gates, metrics (recall, nDCG, bootstrap) |
| `internal/search` | pinned search (ANN or exact) |
| `internal/verify` | full sink-vs-source comparison |
| `migrations/` | versioned SQL for source control objects and the sink schema |
| `embedder/` | FastAPI + sentence-transformers service with pinned revisions |
| `bench/` | freshness, outage, baselines and retrieval harness |
