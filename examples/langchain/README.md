# embedsync LangChain retriever

`embedsync_retriever.EmbedsyncRetriever` is a LangChain `BaseRetriever` over embedsync's
`POST /search` (see [`api/openapi.yaml`](../../api/openapi.yaml)). It returns one `Document` per hit
with `page_content` set to the chunk text and metadata `pipeline`, `key`, `chunk_no`, `score`,
`ranks` (hybrid mode: per-branch rank; empty otherwise) and `generation`.

```python
from embedsync_retriever import EmbedsyncRetriever

retriever = EmbedsyncRetriever(
    "http://127.0.0.1:9187",
    k=5,
    mode="hybrid",  # vector | hybrid | text
    pipelines=["docs"],  # default: all pipelines/views
    filter={"field": "lang", "op": "eq", "value": "en"},
    headers={"X-Tenant-ID": "acme"},  # only when search.tenant_filter is configured
)
docs = retriever.invoke("how do I rotate credentials")
```

A 4xx/5xx response raises `EmbedsyncError` with `.status`, `.code` (for example `invalid_filter`,
`unknown_pipeline`, `search_index_building`) and `.field`.

The tenant header is normally set by a trusted gateway in front of embedsync; pass it in `headers`
only when this process is that trusted caller.

## CLI

```sh
uv run --frozen python example.py --url http://127.0.0.1:9187 --query "rotate credentials" \
  [--mode hybrid] [--k 5] [--pipelines docs,kb] \
  [--filter '{"and":[{"field":"lang","op":"eq","value":"en"},{"field":"year","op":"range","gte":2020}]}'] \
  [--header 'X-Tenant-ID: acme']
```

Exit status: 0 with results, 1 when no results, 2 on a request error.

## Development

```sh
uv run --frozen ruff format --check . && uv run --frozen ruff check .
uv run --frozen pytest -q
```

Dependencies are pinned in `pyproject.toml` and locked in `uv.lock`.
