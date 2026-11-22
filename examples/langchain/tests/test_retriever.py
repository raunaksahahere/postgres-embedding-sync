import json

import httpx
import pytest

from embedsync_retriever import EmbedsyncError, EmbedsyncRetriever

RESPONSE = {
    "generation": "v1",
    "model": "m",
    "revision": "r",
    "provider": "local",
    "mode": "hybrid",
    "exact": False,
    "hits": [
        {
            "pipeline": "docs",
            "key": "42",
            "chunk_no": 0,
            "score": 0.0325,
            "content": "hello",
            "ranks": {"vector": 1, "text": 3},
        },
        {
            "pipeline": "kb",
            "key": "7",
            "chunk_no": 2,
            "score": 0.016,
            "content": "world",
            "ranks": {"text": 1},
        },
    ],
    "embed_ms": 1.0,
    "query_ms": 2.0,
}


def make(handler, **kwargs):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return EmbedsyncRetriever("http://embedsync:9187/", client=client, **kwargs)


def test_request_body_and_headers():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        seen["tenant"] = request.headers.get("X-Tenant-ID")
        return httpx.Response(200, json=RESPONSE)

    flt = {"and": [{"field": "lang", "op": "eq", "value": "en"}]}
    r = make(handler, k=3, mode="text", pipelines=["docs", "kb"], filter=flt, headers={"X-Tenant-ID": "acme"})
    r.invoke("rotate credentials")
    assert seen["method"] == "POST"
    assert seen["url"] == "http://embedsync:9187/search"
    assert seen["body"] == {
        "query": "rotate credentials",
        "k": 3,
        "mode": "text",
        "pipelines": ["docs", "kb"],
        "filter": flt,
    }
    assert seen["tenant"] == "acme"


def test_defaults_omit_optional_fields():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=RESPONSE)

    make(handler).invoke("q")
    assert seen["body"] == {"query": "q", "k": 5, "mode": "hybrid"}


def test_hits_map_to_documents():
    docs = make(lambda _req: httpx.Response(200, json=RESPONSE)).invoke("q")
    assert [d.page_content for d in docs] == ["hello", "world"]
    assert docs[0].metadata == {
        "pipeline": "docs",
        "key": "42",
        "chunk_no": 0,
        "score": 0.0325,
        "ranks": {"vector": 1, "text": 3},
        "generation": "v1",
    }
    assert docs[1].metadata["ranks"] == {"text": 1}


def test_no_hits():
    assert make(lambda _req: httpx.Response(200, json={**RESPONSE, "hits": []})).invoke("q") == []


def test_400_raises_with_code():
    body = {"error": 'unknown field "colour"', "code": "unknown_field", "field": "colour"}
    r = make(lambda _req: httpx.Response(400, json=body))
    with pytest.raises(EmbedsyncError) as exc:
        r.invoke("q")
    assert exc.value.status == 400
    assert exc.value.code == "unknown_field"
    assert exc.value.field == "colour"
    assert "unknown_field" in str(exc.value)


def test_5xx_plain_text_raises():
    r = make(lambda _req: httpx.Response(503, text="backend down"))
    with pytest.raises(EmbedsyncError) as exc:
        r.invoke("q")
    assert exc.value.status == 503
    assert exc.value.code is None
    assert "backend down" in str(exc.value)
