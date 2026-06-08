# ragflow_contracts

Single source of truth for the RAGflow event envelope shared across services
(pdf_reader → text_chunker → embedder → vector_indexer).

Defines the Pydantic models (`IngestMessage`, `ChunkMessage`, `DeletionMessage`)
and the shared helpers that producers and consumers must agree on:

- `compute_doc_id(bucket, object)` — stable document id (used by the chunker to
  stamp chunks and by the indexer to target deletions).
- `chunk_uuid(doc_id, index)` — deterministic chunk UUID → idempotent upserts.
- `tombstone_uuid(doc_id)` — deterministic id for a deletion tombstone.
- `now_ms()` — millisecond timestamps for `published_at` / `deleted_at`, used to
  resolve the "deleted while still being processed" race.

Installed into a service image with `pip install ./contracts` (the service's
build context is the repo root). Import via `import ragflow_contracts`.
