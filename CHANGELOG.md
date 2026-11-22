# Changelog

## v0.6.1

The first release of filtered and hybrid search. The `v0.6.0` tag was created but never
released: its upgrade filled a full-text column by rewriting every existing chunk row, which
re-inserted every vector into the HNSW index and could change approximate results after the
upgrade. v0.6.1 indexes full text with a concurrently built expression index instead, so an
upgrade rewrites no chunk of a pipeline without metadata. Upgrade from v0.5.0 directly to
v0.6.1.

Filtered, full-text and hybrid search: `/search` can rank by full-text match as well as by
embedding, fuse both with reciprocal rank fusion, and restrict results by typed metadata
and by a tenant from a trusted gateway. Design and examples: [docs/search.md](docs/search.md).

### Added

* **Modes**: `mode` selects `vector` (default, unchanged), `text` (PostgreSQL full-text search,
  `websearch_to_tsquery` over `search.text_config`, no embedding call) or `hybrid` (both
  branches fused with reciprocal rank fusion, `score = sum of 1 / (rrf_k + rank)`, default
  `rrf_k` 60 from `search.rrf_k`, overridable per request). Hybrid hits report `ranks`, their
  rank in each branch that returned them.
* **Metadata**: `metadata:` on a pipeline or view declares filterable columns (`text`, `int`,
  `float`, `bool`, `timestamp`, `text_array`). Metadata is stored with every chunk and is not
  embedded: changing it never re-embeds a document.
* **Filters**: a recursive `filter` (`and`, `or`, `not` and the conditions `eq`, `ne`, `in`,
  `range`, `contains`, `exists`), in the POST body or the URL-encoded `filter` parameter of GET.
  A missing field fails every comparison, `ne` included; `not` of a condition matches documents
  without the field.
* **Pipeline restriction**: `pipelines` (POST) and `pipeline` (GET, comma-separated).
* **Tenant scope**: `search.tenant_filter: {field, header}` adds `<field> eq <header>` to every
  branch. Requests without the header get `400 tenant_required`; a filter on the tenant field gets
  `400 tenant_filter_forbidden`. embedsync does not authenticate callers: the header must be set by
  a gateway.
* **Configuration**: `search.text_config` (default `simple`) and `search.rrf_k` (default 60).
* **CLI**: `embedsync search` gains `--mode`, `--filter`, `--pipeline` and `--tenant`.
* **Status**: each generation reports `search` (`building` or `ready`) in `embedsync status`
  (text and `--json`, field `generations[].search`). `embedsync verify` reports
  `meta_stale` per generation and pipeline and `fts_index_missing` per generation.
* **Schema**: `sink/0005_search.sql` adds `meta` (jsonb, GIN-indexed) to every chunk table, and
  `search_target` and `search_fingerprint` to `generations`. Full text uses an expression index
  per chunk table, `<table>_fts_<text_config>` on `to_tsvector(<text_config>, content)`, built
  concurrently by `run`.
* **Examples and docs**: `examples/embedsync-search.yaml` and `examples/seed-search.sql`,
  `docs/search.md`, and the search section of the README. The LangChain and LlamaIndex
  retrievers take `mode`, `pipelines`, `filter` and `headers`.
* **Error bodies**: `/search` errors are JSON `{error, code, field}`. The codes are listed in
  [docs/search.md](docs/search.md#errors) and in [`api/openapi.yaml`](api/openapi.yaml).

### Changed

* **`/search` errors**: client errors are now `400` (invalid request, filter or tenant),
  `404` (unknown generation or pipeline), `405` (method other than GET or POST) and `413`
  (POST body over 1 MiB). Previously every failure was `503`. `503` is kept for `search_index_building` (with `Retry-After`) and
  for backend failures.
* **POST /search** takes a JSON body. A POST that sends the legacy query string (`?q=`) without
  a JSON content type is still answered as before.
* **Responses**: every `/search` response carries `mode`. Hybrid hits carry `ranks`. Other
  fields are unchanged.
* **Snapshot metadata**: metadata columns are read during a snapshot with their type's output
  function, the same text the replication stream sends (`bool` is `t` or `f`, not the output of
  an explicit cast). A backfill and the stream therefore store identical metadata.
* **Metadata validation**: `run` checks each metadata column against its declared type when it
  starts (`float` accepts integer columns; `text` accepts any column).

### Upgrade notes

* Stop `embedsync run`, install the new version, run `embedsync init --admin-url <admin URL>`
  (or the admin DSN variables), then start `run`. `init` applies `sink/0005_search.sql`: it adds
  the column and index above and changes nothing else. Nothing is re-embedded. Adding the
  column is instant; building its GIN index reads each chunk table once and blocks writes to it
  meanwhile (reads, including search, continue), which is why `run` is stopped for `init`.
* After the upgrade, `run` completes search in the background: it builds the full-text
  expression index concurrently (no row is rewritten, so chunks of pipelines without metadata,
  and their vector index, stay exactly as they were) and fills the metadata of pipelines and
  views that declare `metadata:` by a re-scan that reuses the stored vectors (those rows get new
  versions; vacuum large tables afterwards). Until a generation is complete, filtered, text and
  hybrid requests to it answer `503 search_index_building` with `Retry-After: 30`. Vector
  requests without a filter or tenant keep their results. `embedsync status` shows
  `search=building`, then `search=ready`.
* Changing a `metadata:` declaration or `search.text_config` starts the same background rebuild
  for each live generation.
* Downgrading after `init` is not supported.

### Compatibility

* Legacy `GET /search?q=&k=&generation=&exact=` is unchanged: the same hits, the same fields,
  and an out-of-range `k` still falls back to 10 on GET.
* Clients that treated every `/search` failure as a transient `503` must handle `400`, `404`
  and `405`.
* Filtered vector search uses pgvector's iterative index scan, which needs pgvector 0.8.0 or
  later. On an older pgvector a filtered request can return fewer than `k` hits with
  `exact: false`; `exact: true` scans the table instead.

## v0.5.0

Embedding providers: a model can be embedded by an OpenAI-compatible endpoint or the
Gemini API, not only by the built-in embedder.

* **Providers**: `providers:` names extra endpoints. `kind: openai` speaks the
  OpenAI-compatible `/embeddings` API (Ollama, vLLM, HF TEI, OpenAI, NVIDIA's API catalog);
  `kind: gemini` speaks Gemini's `batchEmbedContents`, which sends document and query task
  types. A model selects one with `provider:`, and `request_dimensions` asks for shorter
  vectors. The built-in embedder remains the default: existing configurations do not
  change. See `examples/embedsync-providers.yaml`.
* **Routing**: each generation records its provider (name, kind, endpoint without
  credentials, request settings). Queries are embedded by the searched generation's
  provider with the same settings, so promotion and rollback switch providers together
  with the index. Search results report `provider`; `status` names the provider of
  non-local generations.
* **Limits and faults**: requests per second with a burst, tokens per minute and a
  concurrency bound per provider; batches cut by inputs and estimated tokens. HTTP 429 is
  retried and `Retry-After` is honoured up to `max_retry_after`. Outages and malformed
  responses are retried and never acknowledged. A batch the provider refuses (400, 413,
  422) is bisected, so only the inputs it will never accept are quarantined; a refusal of
  every input is a permanent error. API keys come only from environment variables and are
  redacted from errors.
* **Fingerprints**: a non-local generation's chunk fingerprints include its provider name,
  kind, request dimensions and document input type. Local fingerprints are unchanged.
* **Metrics and cost**: `embedsync_provider_*` series labelled by provider (requests,
  latency, retries, retry wait, inputs, tokens by source). The cost series
  `embedsync_provider_estimated_cost_usd_total{provider,pricing_source}` is an estimate:
  tokens times the configured price, labelled with the configured pricing source.
  embedsync ships no price list.
* **Migrations**: a provider model is migrated like any model, through the same quality
  gates.
* **Upgrade**: stop `embedsync run`, install the new version, run
  `embedsync init --admin-url <admin URL>`, then start `run`. It applies
  `sink/0004_providers.sql` (adds `generations.provider`; additive). Existing generations
  are recorded as built by the built-in embedder and keep serving; nothing is re-embedded.
  Downgrading after `init` is not supported.
* **Verification**: integration tests with a fault-injecting fake provider (429 with
  `Retry-After`, outage, malformed batches, crashes around the sink commit, a cross-provider
  migration with a continuous reader). Gated real-endpoint tests ran against Ollama on an AMD
  GPU (ROCm) and the Gemini free tier; measurements are in `bench/results/providers.json`.
  Not verified: OpenAI itself, NVIDIA, CUDA GPUs and paid tiers.
* **Failure handling**: a single input refused by a provider is quarantined only once the
  provider is known to work (an earlier valid response or a known-good probe); otherwise
  the refusal is a configuration or account error and nothing is quarantined. Five
  invalid responses in a row isolate the inputs responsible; a wrong configured
  dimension stops with an error instead of retrying silently.

## v0.4.0

Document views: one document assembled from related tables.

* **Views**: `views:` defines a root table, ordered child tables (`children`) and rows
  reached through a link table (`links`, many-to-many). The template sees the root
  fields and one list per child and link name. The rendered document is chunked like
  any other. Every key and reference starts with a tenant column, and a table belongs to
  one pipeline or view. See `examples/embedsync-views.yaml`.
* **Consistency**: view tables are kept in `source_state` (plus the new `source_refs`)
  and committed with the checkpoint; the stream path never reads the source. Each
  affected document is rendered once per unit from the state after the unit, so a
  section move, a tag rename or a multi-edit transaction ends as one final document.
  Truncating a child, link or target table re-renders the documents that used it;
  truncating the root table deletes the view's documents.
* **Backfill kinds**: jobs are `embed`, `state` or `render`. A view first scans each
  of its tables into state, then renders its documents per generation. A model
  migration builds the view from state (render job only).
  `embedsync backfill --pipeline <view>` queues both. `embedsync status` lists views
  (`view`) and their tables (`view_table`).
* **verify** assembles view documents from the source with SQL joins, independently of
  the stream code. Child and link rows without a parent document are reported as
  `view_orphan_rows`.
* **Limits**: `max_children` (default 1000) quarantines a larger document as
  `too_many_children`. `max_fanout_per_txn` (default 10000) pauses the pipeline
  (`fanout_limit`) before acknowledging a transaction that changes more documents of a
  view; raise it and restart. Neither is part of the view fingerprint.
* **Upgrade**: stop `embedsync run`, install the new version, run
  `embedsync init --admin-url <admin URL>`, then start `run`. It applies
  `sink/0003_views.sql` (adds `source_refs` and the job kind; additive). Existing
  documents are not re-embedded. Downgrading after `init` is not supported.

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
