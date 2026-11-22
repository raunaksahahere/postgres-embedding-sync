"""LlamaIndex retriever backed by the embedsync ``POST /search`` API."""

from __future__ import annotations

from typing import Any

import httpx
from llama_index.core.retrievers import BaseRetriever
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode


class EmbedsyncError(RuntimeError):
    """A non-2xx response from embedsync, carrying the server's error ``code`` (and ``field``)."""

    def __init__(self, status: int, code: str | None, message: str, field: str | None = None):
        self.status = status
        self.code = code
        self.field = field
        self.message = message
        detail = f"embedsync /search failed: HTTP {status}"
        if code:
            detail += f" {code}"
        if field:
            detail += f" (field {field})"
        super().__init__(f"{detail}: {message}")


def raise_for_error(resp: httpx.Response) -> None:
    """Raise EmbedsyncError for a 4xx/5xx response, using the JSON error body when present."""
    if resp.status_code < 400:
        return
    code = field = None
    message = resp.text.strip()
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        code = body.get("code")
        field = body.get("field")
        message = body.get("error", message)
    raise EmbedsyncError(resp.status_code, code, message, field)


class EmbedsyncRetriever(BaseRetriever):
    """Retrieve nodes from embedsync (vector, text or hybrid search).

    ``headers`` are sent with every request (for example a tenant header such as ``X-Tenant-ID``
    when embedsync runs behind a trusted gateway). ``client`` lets callers (and tests) supply an
    ``httpx.Client``; by default one is created per call.
    """

    def __init__(
        self,
        base_url: str,
        k: int = 5,
        mode: str = "hybrid",
        pipelines: list[str] | None = None,
        filter: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
        **kwargs: Any,
    ):
        self.base_url = base_url
        self.k = k
        self.mode = mode
        self.pipelines = pipelines
        self.filter = filter
        self.headers = headers
        self.timeout = timeout
        self.client = client
        super().__init__(**kwargs)

    def request_body(self, query: str) -> dict[str, Any]:
        body: dict[str, Any] = {"query": query, "k": self.k, "mode": self.mode}
        if self.pipelines:
            body["pipelines"] = list(self.pipelines)
        if self.filter is not None:
            body["filter"] = self.filter
        return body

    def _post(self, client: httpx.Client, query: str) -> dict[str, Any]:
        resp = client.post(
            self.base_url.rstrip("/") + "/search",
            json=self.request_body(query),
            headers=self.headers or {},
            timeout=self.timeout,
        )
        raise_for_error(resp)
        return resp.json()

    def _retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:
        if self.client is not None:
            data = self._post(self.client, query_bundle.query_str)
        else:
            with httpx.Client() as client:
                data = self._post(client, query_bundle.query_str)
        generation = data.get("generation")
        nodes = []
        for hit in data.get("hits") or []:
            node = TextNode(
                text=hit["content"],
                id_=f"{hit['pipeline']}:{hit['key']}:{hit['chunk_no']}",
                metadata={
                    "pipeline": hit["pipeline"],
                    "key": hit["key"],
                    "chunk_no": hit["chunk_no"],
                    "score": hit["score"],
                    "ranks": hit.get("ranks", {}),
                    "generation": generation,
                },
            )
            nodes.append(NodeWithScore(node=node, score=hit["score"]))
        return nodes
