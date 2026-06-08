# tests

Smoke tests — the first stage toward a fuller unit-test suite. They cover the
**pure logic** that's cheap to verify without the live stack (no RabbitMQ /
Weaviate / Ollama needed):

- `test_contracts.py` — `doc_id` / chunk-UUID determinism, envelope `schema` alias, deletion parsing
- `test_mq_batcher.py` — the shared `BatchConsumer` (batch flush, ack/nack, batch_size=1)
- `test_chunker.py` — token offsets/counts, boilerplate stripping
- `test_embedder_batch.py` — batched embedding maps vectors 1:1 (fake Ollama)
- `test_indexer_tombstone.py` — deletion race guard in batched upsert (mock Weaviate)
- `test_obs.py` — logging + metrics helpers degrade gracefully

## Run

```bash
uv venv .venv-test && . .venv-test/bin/activate
uv pip install pytest pydantic-settings tiktoken requests orjson pika prometheus-client
pytest
```

`conftest.py` puts the service packages and the shared `contracts` package on
`sys.path`, so no install/build step is required.

## Not yet covered (see docs/FUTURE_WORK.md)

Integration smoke test (push a PDF → assert a vector lands in Weaviate),
`pdf_reader` markdown→markers mapping, and the rag_api pipeline against a live
Weaviate. These need running services and belong in a CI job with service
containers.
