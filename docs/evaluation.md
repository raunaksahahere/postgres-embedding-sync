# Evaluation methodology

Every result in the README comes from `make bench` (`bench/`, Python 3.12, uv-locked)
and is stored as JSON in `bench/results/` together with the command, seeds, hardware,
software versions and git commit. Charts are regenerated from the JSON by
`embedsync-bench chart`.

## Freshness

* **Stack**: the Compose stack from this repository (project `embedsync-bench`,
  config `bench/embedsync.bench.yaml`): PostgreSQL 17 + pgvector 0.8.7, the CPU
  embedder serving all-MiniLM-L6-v2 at a pinned revision, `embedsync run`.
* **Workload (synthetic)**: `bench/src/embedsync_bench/workload.py`. An open-loop writer
  commits one source transaction per operation at a fixed rate (operation *i* is due at
  `start + i/rate`); 60 % updates, 25 % inserts, 15 % deletes; documents of 60–110 words
  drawn from a fixed vocabulary; 2,000 rows preloaded and backfilled before measuring.
  Every insert/update writes a unique token into the title. Seeded (`--seed`, default 7;
  run *i* uses seed + *i*).
* **Independent reader**: a separate connection as the read-only `embedsync_reader`
  role polls the serving generation's chunk table (resolved through the serving
  catalog, exactly what search reads) every 20 ms for keys with pending writes. A write
  is visible when the sink shows that version or a newer version of the row (updates of
  the same row inside one unit coalesce); a delete is visible when the row is gone. The
  observation time is the start of the polling statement, so the measured latency
  over-estimates by at most one poll interval. Writer and reader share one clock.
* **Reported**: committed writes, achieved rate, p50/p90/p99/max/mean latency, writes
  never visible (must be 0), embeddings computed and embeddings/s (from embedsync's
  `/metrics`), the internal commit→durable mean for comparison.

### Baselines

Same workload, same reader, same embedder and model, run on a fresh stack with
embedsync stopped so the baselines get the whole CPU (`bench/src/embedsync_bench/baselines.py`):

* **trigger + queue**: an `AFTER INSERT OR UPDATE OF id, title, body OR DELETE` trigger
  enqueues row ids into a table; a worker ticks every 10 s (the schedule in the
  [Supabase automatic-embeddings guide](https://supabase.com/docs/guides/ai/automatic-embeddings)),
  drains the queue with `FOR UPDATE SKIP LOCKED` in batches of 64, reads the current
  rows, embeds and upserts or deletes. It re-implements the *pattern*; it is not
  Supabase (no pgmq, pg_net or Edge Functions).
* **periodic batch**: every 60 s, scan the table, re-embed rows whose content hash
  changed and delete rows that vanished (a cron-style re-embed job, run far more often
  than the nightly jobs it stands for).

### Outage

Rate 15 rows/s for 150 s plus a ballast writer inserting ~96 KB rows into an
*unpublished* table at ~20 rows/s (the slot retains all WAL, not only the published
table's). At t = 30 s the embedder container is stopped (`docker compose stop`), at
t = 90 s it is started again (it reloads the models from the cache). A sampler records
the slot's retained WAL (`pg_current_wal_lsn() − restart_lsn`), `wal_status`, and
embedsync's freshness lag and consumer state every second.

## Retrieval quality and live model migration

* **Data**: BEIR SciFact, 5,183 abstracts, downloaded from Hugging Face
  (`BeIR/scifact` revision `984eed82…`, `BeIR/scifact-qrels` revision `2938d17d…`) and
  verified by SHA-256 (`bench/src/embedsync_bench/retrieval.py`). Not redistributed.
* **Queries**: the union of the train and test judgments (1,109 judged queries); 500
  sampled with `random.Random(20261120)`; the sample's SHA-256 is recorded. The
  judgments are held out: no parameter was tuned on them; chunking, ef_search and
  gates come from the quickstart config.
* **Models** (pinned Hugging Face revisions, 384 dimensions each, so pinning is tested
  with equal dimensions): all-MiniLM-L6-v2 (initial), bge-small-en-v1.5 (with its query
  instruction), paraphrase-MiniLM-L3-v2 (expected regression).
* **Gates, declared before running** (`bench/embedsync.bench.yaml`, identical to the
  quickstart config): coverage exact (0 missing/extra/stale chunks), 0 unresolved rows,
  checkpoint past the catch-up barrier, candidate ANN-vs-exact recall@10 ≥ 0.95
  (tie-aware), and non-inferiority: the lower bound of the 95 % paired-bootstrap CI
  (2,000 resamples, seed 20261120) for Δ nDCG@10 and Δ recall@10 (candidate − serving)
  must be ≥ −0.01.
* **Metrics** (`internal/migrate/metrics.go`): document-level recall@10 and nDCG@10
  (chunks deduplicated to the best-scoring chunk per document), both generations
  searched at one sink snapshot. Exact search disables index scans.
* **Live cutover**: a reader issues `/search?k=10` with random sampled queries
  continuously (~10/s) while a writer changes rows (2 rows/s on separate keys) and the
  harness runs `migrate --to bge`, `migrate --to minilm_l3`, `migrate rollback`. It
  records errors, latency and which generation answered; the cutover gap is the time
  between the last answer from the old generation and the first from the new one.
* **CI fixture**: `eval/fixtures/ci` (24 synthetic queries over the 48 demo rows,
  labelled synthetic) exercises the same gate code in the end-to-end test.
