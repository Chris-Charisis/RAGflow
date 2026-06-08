"""Smoke tests for batched embedding (fake Ollama client)."""
import pytest

from embedder.embedder.embedder import Embedder, EmbeddingError


class FakeOllama:
    def __init__(self, dim=3):
        self.dim = dim
        self.calls = 0

    def request(self, method, path, json):
        self.calls += 1
        n = len(json["input"])
        return {"embeddings": [[float(i)] * self.dim for i in range(n)]}


def test_process_batch_embeds_in_one_call_and_maps_vectors():
    client = FakeOllama(dim=4)
    e = Embedder(client=client, model="m")
    payloads = [{"chunk": {"text": "a"}}, {"chunk": {"text": "b"}}, {"chunk": {"text": "c"}}]
    out = e.process_batch(payloads)
    assert client.calls == 1                       # batched into a single call
    assert len(out) == 3
    assert out[0]["embedding"]["embedding_model"] == "m"
    assert out[0]["embedding"]["embedding_dim"] == 4
    # list-of-vectors shape the indexer expects (extract_vector reads [0])
    assert out[1]["embedding"]["embedding_vector"] == [[1.0, 1.0, 1.0, 1.0]]


def test_invalid_payload_raises():
    e = Embedder(client=FakeOllama(), model="m")
    with pytest.raises(EmbeddingError):
        e.process_batch([{"no_chunk": True}])


def test_count_mismatch_raises():
    class BadOllama:
        def request(self, *a, **k):
            return {"embeddings": [[0.0]]}  # one vector for two inputs
    e = Embedder(client=BadOllama(), model="m", max_retries=1)
    with pytest.raises(EmbeddingError):
        e.process_batch([{"chunk": {"text": "a"}}, {"chunk": {"text": "b"}}])
