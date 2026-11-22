"""LangChain retriever backed by the embedsync ``POST /search`` API."""

from __future__ import annotations

from typing import Any

import httpx
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict


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
    """Retrieve documents from embedsync (vector, text or hybrid search).

    ``headers`` are sent with every request (for example a tenant header such as ``X-Tenant-ID``
    when embedsync runs behind a trusted gateway). ``client`` lets callers (and tests) supply an
    ``httpx.Client``; by default one is created per call.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    k: int = 5
    mode: str = "hybrid"
    pipelines: list[str] | None = None
    filter: dict[str, Any] | None = None
    headers: dict[str, str] | None = None
    timeout: float = 30.0
    client: httpx.Client | None = None

    def __init__(self, base_url: str, **kwargs: Any):
        super().__init__(base_url=base_url, **kwargs)

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

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> list[Document]:
        if self.client is not None:
            data = self._post(self.client, query)
        else:
            with httpx.Client() as client:
                data = self._post(client, query)
        generation = data.get("generation")
        return [
            Document(
                page_content=hit["content"],
                metadata={
                    "pipeline": hit["pipeline"],
                    "key": hit["key"],
                    "chunk_no": hit["chunk_no"],
                    "score": hit["score"],
                    "ranks": hit.get("ranks", {}),
                    "generation": generation,
                },
            )
            for hit in data.get("hits") or []
        ]
