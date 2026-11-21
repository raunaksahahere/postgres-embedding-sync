import numpy as np
from fastapi.testclient import TestClient

from embedder.app import Settings, create_app


class FakeModel:
    def __init__(self, dim=4):
        self.dim = dim

    def get_embedding_dimension(self):
        return self.dim

    def encode(self, inputs, **_):
        out = np.array([[float(len(t)), 1.0, 0.0, 0.0] for t in inputs], dtype="float32")
        return out / np.linalg.norm(out, axis=1, keepdims=True)


def client(**kw):
    s = Settings(
        models=[("m", "abcdef1")],
        max_input_chars=kw.get("max_chars", 50),
        max_batch=8,
        threads=None,
        fault_reject=kw.get("fault"),
    )
    app = create_app(s, loader=lambda n, r: FakeModel(), background=False)
    return TestClient(app)


def test_health_and_embed():
    with client() as c:
        assert c.get("/healthz").json()["models"] == ["m@abcdef1"]
        r = c.post("/v1/embed", json={"model": "m", "revision": "abcdef1", "inputs": ["a", "bb"]})
        assert r.status_code == 200
        body = r.json()
        assert body["dimension"] == 4 and body["revision"] == "abcdef1"
        assert len(body["embeddings"]) == 2
        assert abs(sum(x * x for x in body["embeddings"][0]) - 1) < 1e-5


def test_unknown_revision_is_refused():
    with client() as c:
        r = c.post("/v1/embed", json={"model": "m", "revision": "fffffff", "inputs": ["a"]})
        assert r.status_code == 404


def test_rejects_name_indexes():
    with client(fault="POISON") as c:
        inputs = ["ok", "x" * 60, "a POISON b"]
        r = c.post("/v1/embed", json={"model": "m", "revision": "abcdef1", "inputs": inputs})
        assert r.status_code == 422
        assert r.json()["detail"]["rejected"] == [1, 2]


def test_batch_limit():
    with client() as c:
        r = c.post("/v1/embed", json={"model": "m", "revision": "abcdef1", "inputs": ["a"] * 9})
        assert r.status_code == 413


def test_settings_require_pinned_revision():
    import os

    import pytest

    os.environ["EMBEDDER_MODELS"] = "sentence-transformers/all-MiniLM-L6-v2"
    with pytest.raises(ValueError):
        Settings.from_env()
    os.environ["EMBEDDER_MODELS"] = "a/b@1110a243fdf4706b3f48f1d95db1a4f5529b4d41"
    assert Settings.from_env().models == [("a/b", "1110a243fdf4706b3f48f1d95db1a4f5529b4d41")]


def test_bundled_path(tmp_path):
    from embedder.app import bundled_path

    model = tmp_path / "org__name" / "abc1234"
    model.mkdir(parents=True)
    assert bundled_path("org/name", "abc1234", str(tmp_path)) is None  # not a model dir yet
    (model / "modules.json").write_text("[]")
    assert bundled_path("org/name", "abc1234", str(tmp_path)) == str(model)
    assert bundled_path("org/name", "other", str(tmp_path)) is None
    assert bundled_path("org/name", "abc1234", "") is None
