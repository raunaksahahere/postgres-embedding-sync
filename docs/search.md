# Search: modes, metadata filters and tenant scope

`POST /search` (and `GET /search`) on the metrics listener (`metrics.listen`, default
`:9187`) answers queries over the pipelines and views of one generation. Three modes are
available, and any of them can be restricted by pipeline, by a metadata filter, and by a
tenant taken from a gateway header. The machine-readable contract is
[`api/openapi.yaml`](../api/openapi.yaml); this page explains what the server does with it.

Contents: [modes](#modes) · [declaring metadata](#declaring-metadata) · [filter language](#filter-language) ·
[pipeline restriction](#pipeline-restriction) · [tenant scope](#tenant-scope) · [consistency](#consistency) ·
[GET compatibility](#get-compatibility) · [errors](#errors) · [text_config](#text_config) ·
[upgrading](#upgrading-an-existing-installation) · [pgvector versions](#pgvector-versions) ·
[examples](#examples)

## Modes

The request field `mode` (or the `mode` query parameter) selects the retrieval branches.
The default is `vector`.

| mode | what runs | score |
|---|---|---|
| `vector` | embedding similarity against the chunk vectors. The query is embedded by the searched generation's own encoder. Uses the ANN index unless `exact` is true. | `1 - cosine distance` for cosine generations, `-distance` for l2 and inner product |
| `text` | PostgreSQL full-text search (`websearch_to_tsquery` matched against `to_tsvector(search.text_config, content)` of each chunk, served by an expression index). The query is not embedded. | `ts_rank_cd` of the document's best matching chunk |
| `hybrid` | both branches, fused with reciprocal rank fusion (RRF) | the fused score below |

**Hybrid.** Each branch returns up to `depth = max(4 * k, 50)` documents (one best chunk per
document and pipeline). A document's fused score is the sum over the branches that returned it:

```
score(d) = sum over branches b that return d of  1 / (rrf_k + rank_b(d))
```

`rank_b(d)` is the 1-based position of `d` in branch `b`. `rrf_k` defaults to
`search.rrf_k` (60 unless configured; valid range 1 to 1000) and can be overridden per request
with `rrf_k` (1 to 1000). Hits are sorted by score, then by pipeline, then by key. The response
gives each hit a `ranks` object with the branch ranks that exist, for example
`{"vector": 1, "text": 3}`; a branch that did not return the document is omitted. A document
found only by the text branch has only `text`. `ranks` is present only in hybrid mode.

Both branches of a hybrid request read the same snapshot (see [consistency](#consistency)).

## Declaring metadata

Metadata is a set of columns of the source row that embedsync stores with every chunk of the
document, so filters can use them. Declare them under `metadata:` on a pipeline or on a view
(in a view, the columns are those of the root table):

```yaml
pipelines:
  - name: articles
    table: public.articles
    key: id
    fields: [{name: title, column: title}, {name: body, column: body}]
    metadata:
      - {name: lang, column: lang, type: text}
      - {name: year, column: year, type: int}
      - {name: tags, column: tags, type: text_array}
```

Each entry has `name` (the filter field name), `column` (the source column) and `type`. `name`
and `column` must be lowercase identifiers. The rules:

* Types are `text`, `int`, `float`, `bool`, `timestamp` and `text_array`.
* The column must exist on the table and have a compatible type (checked when `run` starts):

  | declared type | accepted column types |
  |---|---|
  | `text` | any column (its text form is stored) |
  | `int` | `smallint`, `integer`, `bigint` |
  | `float` | `real`, `double precision`, `numeric`, and also the integer types |
  | `bool` | `boolean` |
  | `timestamp` | `date`, `timestamp`, `timestamp with time zone` |
  | `text_array` | `text[]`, `varchar[]` |

* A field name has one type everywhere. Pipelines and views searched together share one
  filter schema, so the same name cannot be `int` in one and `text` in another.
* Metadata is not rendered and not embedded. Changing a metadata column updates the stored
  metadata of its document without re-embedding it. Text search columns are rebuilt
  when the declarations change (see [upgrading](#upgrading-an-existing-installation)).

## Filter language

A filter is a JSON object, passed as the `filter` field of the POST body, or URL-encoded in the
`filter` query parameter of GET:

```
filter    := {"and": [filter, ...]} | {"or": [filter, ...]} | {"not": filter} | condition
condition := {"field": NAME, "op": OP, ...operands}
```

`and` and `or` take a non-empty array. `not` takes one filter. Each object has exactly one of
these keys. Field names must be declared under `metadata:`.

| op | operands | matches when | example |
|---|---|---|---|
| `eq` | `value` | the field equals the value | `{"field":"lang","op":"eq","value":"en"}` |
| `ne` | `value` | the field is present and differs from the value | `{"field":"lang","op":"ne","value":"de"}` |
| `in` | `values` (1 to 1000 entries) | the field equals one of the values | `{"field":"lang","op":"in","values":["fr","de"]}` |
| `range` | at least one of `gt`, `gte`, `lt`, `lte` | the field satisfies every bound given | `{"field":"year","op":"range","gte":2018,"lt":2021}` |
| `contains` | `value` (a string) | a `text_array` field contains the value | `{"field":"tags","op":"contains","value":"t1"}` |
| `exists` | none | the field is present (not null) | `{"field":"year","op":"exists"}` |

Combined example, with `and`, `not` and `or`:

```json
{"and": [
  {"field": "lang", "op": "in", "values": ["en", "de"]},
  {"not": {"field": "org", "op": "eq", "value": "org0"}},
  {"or": [
    {"field": "featured", "op": "eq", "value": true},
    {"field": "rating", "op": "range", "gt": 5.5}
  ]}
]}
```

Operand values must match the field type:

* `text`: JSON string. `int`: integral JSON number (64-bit range). `float`: JSON number.
* `bool`: JSON `true` or `false`. `timestamp`: string, RFC 3339 (`2024-02-02T10:00:00+05:30`)
  or `YYYY-MM-DD`. Timestamps compare as instants.
* Text ranges compare bytewise (`COLLATE "C"`), so `"B" < "a"`.
* `range` is not supported on `bool` or `text_array` fields. `contains` only on `text_array`.
  A `text_array` field supports only `contains` and `exists`.
* `range` may not combine `gt` with `gte`, or `lt` with `lte`.
* Unknown keys in a condition are rejected (for example `"value"` on an `exists` condition).

**Missing fields.** A document without a key for the field:

* fails every comparison (`eq`, `in`, `range`, `contains`);
* never matches `ne`, because `ne` requires the field to be present;
* fails `exists`.

`not` negates its operand under the same rule: the operand is treated as false when the field
is missing, so `{"not": {"field": "org", "op": "eq", "value": "org0"}}` matches documents whose
`org` is `org1` **and** documents with no `org`. If you want the field to be present, add an
`exists` condition.

**Limits.** Nesting depth at most 8 (a bare condition is depth 1), at most 100 conditions per
filter, at most 1000 values in one `in`. Exceeding them is `too_complex` (400).

A filter is validated before anything runs, and its values are sent to PostgreSQL as bound
parameters, never as SQL text.

## Pipeline restriction

`pipelines` (POST, an array) or `pipeline` (GET, comma-separated) limits results to the named
pipelines and views. Without it, every pipeline and view is searched. Duplicates are ignored.
A name that is not configured is `404 unknown_pipeline`. Restriction does not require metadata,
and it works in every mode.

## Tenant scope

A deployment that serves several tenants from one index configures a forced tenant predicate:

```yaml
search:
  tenant_filter: {field: tenant_id, header: X-Tenant-ID}
```

* `field` must be a declared metadata field of type `text` or `int`. `header` is required.
* When it is set, every `/search` request must carry the header (GET and POST), or the
  request fails with `400 tenant_required` (`field` is the header name). An empty or
  whitespace-only header counts as missing. For an `int` field the header must be an integer,
  otherwise `400 invalid_value`.
* embedsync adds `<field> eq <header value>` to **every branch** (vector, text and each part
  of a hybrid query). A tenant's results therefore never include another tenant's chunks, even
  for hybrid ranks and scores.
* A request whose filter refers to the tenant field, anywhere in its tree, is refused with
  `400 tenant_filter_forbidden`. The client cannot widen or replace the tenant predicate.

**Trust boundary.** embedsync does not authenticate callers. The header is trusted as
given. It must be set by a gateway that authenticates the caller, and the gateway must
**overwrite** any `X-Tenant-ID` the client sends rather than pass it through. Anyone who can
reach the metrics listener directly can send any header, so the listener must not be exposed to
end users. Bind it to a private address (the default is `:9187`, all interfaces, so set
`metrics.listen` accordingly) and let only the gateway reach it.

## Consistency

A search is pinned to one generation for its whole duration:

1. The generation is chosen: the `generation` field if given, otherwise the serving generation.
2. For vector and hybrid requests, the query is embedded with that generation's encoder (model,
   revision, query prompt, provider).
3. All branches run inside one read-only, repeatable-read transaction. The transaction reads the
   generation again. If it is still the one embedded with, the vector is reused. If a promotion
   or rollback happened in between, the query is re-embedded with the newly serving
   generation's encoder, and the whole request uses that generation.
4. The response's `generation`, `model`, `revision` and `provider` describe the generation that
   answered. A hybrid request never mixes branches from two generations.

An explicit `generation` never changes under a request. Promotion of a new model while searches
run therefore produces results from one encoder and one snapshot each time.

## GET compatibility

The legacy form is unchanged for requests that use only `q`, `k`, `generation` and `exact`:

```
GET /search?q=rotate%20credentials&k=5&generation=v1&exact=true
```

The GET form also accepts `mode`, `pipeline` (comma-separated), `filter` (URL-encoded JSON) and
`rrf_k`. The same restrictions and errors apply as in POST.

Two differences from POST, kept for compatibility: an out-of-range `k` on GET (outside 1 to 100,
or not a number) silently becomes 10, while POST answers `400 invalid_request`. And `exact` is
true only for the literal value `true`.

Requests with a JSON body use `POST /search`. Unknown fields in the body are rejected
(`400 invalid_request`), and bodies are limited to 1 MiB.

Request fields (POST):

| field | default | notes |
|---|---|---|
| `query` | required | 1 to 4096 bytes |
| `k` | 10 | 1 to 100 |
| `mode` | `vector` | `vector`, `text`, `hybrid` |
| `pipelines` | all | see [pipeline restriction](#pipeline-restriction) |
| `generation` | serving | |
| `exact` | false | vector branch without the ANN index |
| `filter` | none | see [filter language](#filter-language) |
| `rrf_k` | `search.rrf_k` (60) | hybrid only; 1 to 1000 |

Response fields: `generation`, `model`, `revision`, `provider`, `mode`, `exact`, `hits`,
`embed_ms`, `query_ms`. A hit has `pipeline`, `key` (a composite key is a JSON array encoded as a
string), `chunk_no`, `score`, `content`, and `ranks` (hybrid only). Hits are one per document.

## Errors

Every error body is `{"error": "<message>", "code": "<code>", "field": "<optional>"}`.

| status | code | when |
|---|---|---|
| 400 | `invalid_request` | malformed or unknown JSON field, missing or oversized `query`, `k` out of range (POST), unknown `mode`, `rrf_k` out of range or not an integer |
| 400 | `invalid_filter` | the filter is not valid JSON, is not an object, has an empty `and`/`or`, has duplicate keys, or a condition has no `field` or `op` |
| 400 | `unknown_field` | the filter names a field that no pipeline or view declares |
| 400 | `unsupported_op` | unknown `op`, or an op the field type does not support (for example `range` on `bool`) |
| 400 | `invalid_value` | an operand does not match the field type; a non-integer tenant header for an `int` tenant field |
| 400 | `too_complex` | nesting deeper than 8, more than 100 conditions, or more than 1000 `in` values |
| 400 | `tenant_required` | `search.tenant_filter` is set and the header is missing or empty |
| 400 | `tenant_filter_forbidden` | the filter refers to the tenant field |
| 404 | `unknown_generation` | the named generation does not exist |
| 404 | `unknown_pipeline` | a named pipeline or view is not configured |
| 405 | `method_not_allowed` | a method other than GET or POST (the response has `Allow: GET, POST`) |
| 413 | `body_too_large` | a POST body over 1 MiB |
| 503 | `search_index_building` | filtered, text or hybrid search on a generation whose search columns are not complete yet. The response has `Retry-After: 30`. |
| 503 | `backend` | a database or embedding provider failure, or no generation is serving yet |

`field` names the offending filter field, parameter or header when one applies. The error
classes are the same for GET and POST.

Filtered, text and hybrid requests are refused before any embedding call, so a 503 for a
building generation costs nothing.

Vector requests without a filter or tenant (pipeline restriction alone is allowed) keep
working while the search columns are being built.

## text_config

`search.text_config` names the PostgreSQL text search configuration of the full-text index
(`<chunk table>_fts_<config>`, on `to_tsvector(<config>, content)`) and of the text branch. The default is `simple`. It must be a configuration that exists in
the sink database (`run` refuses to start otherwise).

`simple` does not stem words and has no stop-word list, so the tokens of the query match the
tokens of the text as written (lowercased). An identifier such as `SKU-48213` is found exactly
and a common word such as `the` is not dropped. `english` stems words (`rotating` matches
`rotate`) and drops stop words, which suits prose and is a poor fit for codes and identifiers.

Changing `text_config` rebuilds the search columns of every live generation, the same way as a
change of metadata declarations.

## Upgrading an existing installation

Search needs one new column on each chunk table, `meta` (the metadata, `jsonb`, GIN-indexed),
and a full-text expression index. Neither rewrites existing rows for pipelines without
metadata, so their vector index and results stay exactly as they were. The upgrade is:

1. Stop `embedsync run`, install the new version.
2. Run `embedsync init --admin-url <admin URL>` (or the admin DSN variables). It applies
   `sink/0005_search.sql`: it adds the `meta` column (instant: a constant default) with its GIN
   index, and the generation columns `search_target` and `search_fingerprint`. Existing rows
   get an empty `meta`. Nothing is re-embedded.
3. Start `embedsync run`. It fills the search columns in the background:
   * pipelines and views that declare `metadata:` are re-scanned from the source, reusing the
     stored vectors, so no input is embedded again;
   * the full-text expression index is built with `CREATE INDEX CONCURRENTLY` (writes
     continue; no row is rewritten);
   * the rows of re-scanned pipelines are rewritten with their metadata (new row versions;
     vacuum large tables afterwards);
   * when a generation's columns are complete for the current configuration, its search state
     becomes `ready`.

Until then, a generation answers filtered, text and hybrid requests with
`503 search_index_building` and `Retry-After: 30`. Vector requests without a filter or tenant
keep their results, which are unchanged. Clients should retry after the `Retry-After` interval
or fall back to vector search.

The same rebuild runs when `search.text_config` changes, or when a `metadata:` declaration
changes (added, removed, renamed, or retyped), and again for each live generation. Only the
pipelines and views that declare metadata are re-scanned.

Progress is visible in two places:

* `embedsync status` prints `search=building` or `search=ready` on each generation line. The
  JSON report has the same value per generation in `generations[].search`.
* `embedsync verify` prints `search columns: meta_stale=N fts_index_missing=false` per
  generation, or `search columns: building (metadata and full-text index not checked)` while the
  rebuild runs.

A downgrade after `init` is not supported (the same rule as the earlier schema upgrades).

## pgvector versions

Filtered vector search uses pgvector's **iterative index scan**. For a request with a filter, a
tenant predicate or a pipeline restriction, embedsync sets `hnsw.iterative_scan =
relaxed_order` (or `ivfflat.iterative_scan` for an IVFFlat generation) for the transaction. The
index then keeps walking until enough rows pass the filter. This needs **pgvector 0.8.0 or
later**. On an older pgvector the setting is unknown and is ignored, so the ANN scan examines only
its first candidates. A selective filter can then return fewer than `k` hits, with
`"exact": false` in the response.

Two ways around it:

* upgrade to pgvector 0.8 or later (the Compose demo runs pgvector 0.8.7);
* send `"exact": true` for the request. The vector branch then scans the table without the index.
  It returns every match, at the cost of a sequential scan.

Text search and the full-text branch do not depend on the pgvector version.

## Examples

Filtered vector search, one pipeline:

```sh
curl -s http://127.0.0.1:9187/search -H 'Content-Type: application/json' -d '{
  "query": "how do I rotate credentials",
  "k": 5,
  "pipelines": ["docs"],
  "filter": {"field": "lang", "op": "eq", "value": "en"}
}'
```

Hybrid search over two pipelines, with a filter, through a gateway that sets the tenant header:

```sh
curl -s http://127.0.0.1:9187/search \
  -H 'Content-Type: application/json' -H 'X-Tenant-ID: acme' -d '{
  "query": "how do I rotate credentials",
  "k": 5,
  "mode": "hybrid",
  "pipelines": ["docs", "kb"],
  "filter": {"and": [
    {"field": "lang", "op": "eq", "value": "en"},
    {"field": "year", "op": "range", "gte": 2020, "lt": 2025}
  ]},
  "rrf_k": 60
}'
```

An exact identifier through the text branch (the query is not embedded):

```sh
curl -s http://127.0.0.1:9187/search -H 'Content-Type: application/json' \
  -d '{"query": "SKU-48213", "k": 3, "mode": "text"}'
```

The same filtered query over GET, with the filter URL-encoded:

```sh
curl -s -G http://127.0.0.1:9187/search --data-urlencode 'q=shared meadow' \
  --data-urlencode 'mode=text' --data-urlencode 'pipeline=articles' \
  --data-urlencode 'filter={"field":"lang","op":"eq","value":"de"}'
```

A runnable configuration with metadata and a search block is in
[`examples/embedsync-search.yaml`](../examples/embedsync-search.yaml), with sample rows in
[`examples/seed-search.sql`](../examples/seed-search.sql).

The LangChain and LlamaIndex retrievers wrap `POST /search`:

* [`examples/langchain`](../examples/langchain/README.md): `EmbedsyncRetriever` is a LangChain
  `BaseRetriever`. Its documents carry `pipeline`, `key`, `chunk_no`, `score`, `ranks` and
  `generation` as metadata.
* [`examples/llamaindex`](../examples/llamaindex/README.md): `EmbedsyncRetriever` is a LlamaIndex
  `BaseRetriever`. It returns `NodeWithScore` objects; the node score is the embedsync score.

Both take `mode` (`vector`, `hybrid`, `text`), `pipelines`, `filter` and `headers` (for the tenant
header, only when the calling process is the trusted gateway). A 4xx or 5xx response raises
`EmbedsyncError` with `.status`, `.code` and `.field`. Their CLIs (`example.py`) take the same
options.

The `embedsync search` command runs the same search from the CLI:

```sh
embedsync search --mode hybrid --pipeline articles --k 5 \
  --filter '{"field":"lang","op":"eq","value":"en"}' "shared summit"
```

It accepts `--k`, `--generation`, `--exact`, `--mode` (`vector`, `text`, `hybrid`), `--filter`
(JSON), `--pipeline` (comma-separated) and `--tenant` (the tenant value, when `search.tenant_filter`
is configured).
