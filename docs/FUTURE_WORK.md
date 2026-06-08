# RAGflow — Future Work & Known Issues

Status log for the v2.0 effort. Captures the recommendation bullets **not yet
implemented** and the **issues that surfaced** while implementing the rest, each
with a suggested approach. Sections map to the original architecture review
(A–E). Items already shipped are listed under "Done" for context.

Legend: 🔴 high priority · 🟠 medium · 🟡 low.

---

## Done so far (v2.0)

- **A** — retrieval+generation path (`rag_api`), Streamlit UI (`rag_ui`).
- **B** — deterministic-UUID upsert, deletion consumer with tombstone race-guard,
  flat filterable Weaviate schema, shared `ragflow_contracts` envelope.
- **C** — token-aware chunking + char offsets, optional boilerplate stripping,
  query-only embedding prompt, Ragas eval markers + `rag_eval` harness.
- **D (partial)** — configurable **batching** (`BATCH_SIZE`/`BATCH_FLUSH_SECONDS`)
  in embedder + vector_indexer; parse-each-PDF-once (PyMuPDF4LLM).
- **E (partial)** — shared `mq`/`obs` modules (de-dup), structured JSON logging +
  Prometheus metrics, smoke tests, hygiene (untracked `text_chunker/outputs`,
  removed DMR compose, dropped dead deps/imports).
- uv conversion across all services (reproducible builds).

---

## Section D — Throughput & messaging (remaining)

### 🔴 D1. Dead-letter queues (DLQ)
**Problem.** Failure handling is still inconsistent: the batch consumers
`nack(requeue=True)` (poison-message loop risk) and deletions `nack(requeue=False)`
(silent drop). No quarantine for messages that can never succeed.

**Approach.**
- Declare each work queue with `x-dead-letter-exchange` → a `*.dlx` exchange and a
  `*.dlq` queue (do this in `ragflow_contracts.mq.declare`, add a `dlq=True` flag).
- Add a redelivery counter (`x-delivery-count` via quorum queues, or a custom
  header incremented on requeue). After K attempts, `nack(requeue=False)` so the
  broker routes to the DLQ instead of looping.
- Prefer **quorum queues** (`x-queue-type: quorum`) — native delivery-count and
  poison-message handling.
- Add a tiny DLQ inspector (or just the RabbitMQ UI) and a replay script.

### 🟠 D2. Horizontal scaling / autoscaling
**Problem.** `container_name` is hardcoded on every service → can't
`docker compose up --scale`. No autoscaling.

**Approach.**
- Remove `container_name`; let Compose/K8s name replicas. Consumers already
  scale horizontally (competing consumers on the same queue).
- On Kubernetes, use **KEDA** with the RabbitMQ scaler (target queue depth) to
  scale embedder/vector_indexer; HPA on CPU for `rag_api`.
- Ensure prefetch ≥ batch_size per replica (already wired) so work spreads.

### 🟠 D3. Push-based ingestion (MinIO notifications)
**Problem.** `pdf_reader` polls and re-lists the whole bucket every interval —
O(bucket) per poll.

**Approach.**
- Configure MinIO bucket notifications (`s3:ObjectCreated:*`, `s3:ObjectRemoved:*`)
  to publish straight to a RabbitMQ exchange (MinIO has an AMQP target).
- `pdf_reader` consumes object-created events (process that key) and
  object-removed events (emit the deletion message), keeping the periodic full
  scan only as a low-frequency reconciliation safety net.
- Keep the processed-marker idempotency so replays are safe.

---

## Section E — Platform concerns (remaining)

### 🟠 E1. Finish shared-code de-duplication
**Done:** RabbitMQ connect/declare/publish + batching (`mq`), logging/metrics
(`obs`), and the envelope (`contracts`) are shared.
**Remaining.** The per-service `Settings` classes still repeat the RabbitMQ block.

**Approach.** Add a `RabbitMQSettings` mixin (and `ObsSettings` with
`metrics_port`, `batch_size`, `batch_flush_seconds`) in `ragflow_contracts`, and
have each service's `Settings` inherit it. Watch the `--no-deps` install caveat
(see I4).

### 🟠 E2. Distributed tracing
**Done:** structured logs + Prometheus counters/histograms; `rag_api` `/metrics`.
**Remaining.** No cross-service tracing; consumer services expose metrics but no
`/health`.

**Approach.**
- Add **OpenTelemetry** SDK to `obs`; instrument the MQ publish/consume points and
  propagate a trace/`doc_id` context via message headers so one document can be
  followed pdf_reader → … → vector_indexer. Export OTLP to a collector
  (Tempo/Jaeger).
- Add a lightweight `/health` (and `/ready`) HTTP endpoint to consumer services
  (a background thread, or reuse the metrics server port).
- Add a Prometheus + Grafana stack to compose (scrape `METRICS_PORT` on each
  service) with a starter dashboard (queue depth, processed/failed rates, batch
  sizes, query latency).

### 🔴 E3. Full unit tests + CI
**Done:** smoke tests (`tests/`) over pure logic.
**Remaining.** No CI; no integration test; coverage is thin.

**Approach.**
- GitHub Actions: (1) lint (ruff) + `pytest` on every PR; (2) `docker compose build`
  to keep `--frozen` locks honest; (3) an **integration smoke test** using service
  containers (RabbitMQ + Weaviate + a stub embedder) that pushes a small PDF and
  asserts a vector lands in Weaviate and `rag_api /query` returns it.
- Grow unit coverage: `pdf_reader` markdown→markers (extract the static method to a
  pure module first), chunker edge cases, `rag_api` pipeline with a fake retriever.

### 🟠 E4. Secrets & TLS
**Problem.** Weaviate API keys are hardcoded in `compose.yaml`; `.env.example`
ships weak creds; no TLS (`minio_secure=False`, plaintext Weaviate/RabbitMQ).

**Approach.** Move secrets to Docker secrets / SOPS-encrypted env (or Vault on
K8s). Enable TLS for MinIO, Weaviate, RabbitMQ; set `MINIO_SECURE=true`. Rotate the
sample keys and document generation.

### 🟠 E5. Kubernetes / Helm
**Problem.** Single Compose file only.

**Approach.** Author a Helm chart (Deployment per service, StatefulSets for
RabbitMQ/Weaviate/MinIO/Ollama, Secrets, KEDA ScaledObjects from D2). The uv
`--frozen` images make this reproducible. Keep Compose for local dev.

### 🟡 E6. Remaining hygiene
- The 153 committed chunk JSONs are now untracked + gitignored; consider deleting
  the on-disk `text_chunker/outputs/` and `example_input.json` test fixtures, or
  move them under `tests/fixtures/`.
- Trim dev-only deps from runtime images (`ipykernel` in pdf_reader/text_chunker).
- Reconcile the two READMEs' getting-started section with the v2 services.

---

## Issues surfaced during implementation

### 🔴 I1. `pdf_reader.close_rabbitqm` typo
`cli.py` calls `pdf_reader.close_rabbitmq()` but the method is spelled
`close_rabbitqm()` → `AttributeError` on shutdown. **Fix:** rename the method to
`close_rabbitmq` (and keep an alias if needed).

### 🟠 I2. Batch upsert relies on Weaviate UUID-overwrite semantics
`upsert_many` uses `collection.batch.dynamic()` with explicit UUIDs and assumes
batch import overwrites on UUID match (idempotent). **Validate against a live
Weaviate.** If batch errors on existing UUIDs instead of overwriting, fall back to
splitting the batch into exists→replace / new→insert, or delete-by-ids first.
(Deterministic UUIDs make a full-batch requeue safe regardless.)

### 🟠 I3. Tombstone check is one query per chunk
`upsert_many` calls `fetch_object_by_id` per chunk for the race guard → N reads per
batch. **Optimize:** batch-fetch tombstones by the distinct `doc_id`s in the batch
(usually 1), or cache tombstones for a short TTL in-process.

### 🟠 I4. `contracts` installed with `--no-deps`
Dockerfiles run `uv pip install --no-deps ./contracts`, so `contracts`' own deps
(pydantic; prometheus optional) are **not** pulled — they must already be in the
service lock. Today that holds (pydantic via pydantic-settings; prometheus-client
added to the 5 long-running services). **Risk:** a future service that imports
`ragflow_contracts` without those deps will fail at import. **Fix when E1 lands:**
either make `contracts` a proper locked workspace dependency, or document the
required peer deps.

### 🟡 I5. Embedder drops malformed messages in a batch
A message with invalid JSON inside a batch is logged and **acked** (dropped),
because batch-level acking can't single one out. Acceptable, but once DLQ (D1)
exists, route malformed messages there instead.

### 🟡 I6. Metrics ports not published to host
Every service uses `METRICS_PORT=9100` internally; they're scrapeable within the
compose network but not mapped to the host (port clash). The Prometheus stack in
E2 will scrape them in-network; no per-service host mapping needed.

### 🟡 I7. OCR-less PDF extraction
PyMuPDF4LLM has no OCR, so scanned/image-only PDFs extract empty. Add a fallback:
if a page yields too little text, route that document through an OCR path
(Tesseract, or `unstructured`/Docling as an opt-in heavy profile).

### 🟡 I8. Misc
- `requires-python = ">=3.12"` is open-ended; pin `<3.13` if you want to bound the
  resolver (the pdf_reader lock already had to reason across 3.14 splits).
- `rag_ui` collects sources via a function attribute on the stream generator —
  works, but a small dataclass/closure would be cleaner.
- `delete_many` removes up to Weaviate's per-call cap (~10k) per deletion; a doc
  with more chunks would need pagination (not a concern at current chunk sizes).
- v1 `recursive_with_overlap` collections are schema-incompatible with the v2 flat
  schema — drop/rename the collection on upgrade.
