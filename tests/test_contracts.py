"""Smoke tests for the shared message contract."""
from ragflow_contracts import (
    Chunk,
    ChunkMessage,
    DeletionMessage,
    Source,
    chunk_uuid,
    compute_doc_id,
    now_ms,
    tombstone_uuid,
)


def test_doc_id_strips_pdf_and_joins_bucket():
    assert compute_doc_id("pdf-data", "papers/arxiv.pdf") == "pdf-data/papers/arxiv"
    assert compute_doc_id("", "x.PDF") == "/x"  # case-insensitive suffix
    assert compute_doc_id("b", None) == ""


def test_chunk_uuid_is_deterministic_and_index_sensitive():
    did = "b/doc"
    assert chunk_uuid(did, 0) == chunk_uuid(did, 0)
    assert chunk_uuid(did, 0) != chunk_uuid(did, 1)
    assert tombstone_uuid(did) != chunk_uuid(did, 0)


def test_chunk_message_serializes_with_schema_alias():
    m = ChunkMessage(
        published_at=now_ms(),
        source=Source(bucket="b", object="o.pdf", etag="e"),
        doc_id="b/o",
        chunk=Chunk(index=0, text="hello", start=0, end=5),
        chunk_strategy="tokens", chunk_size=400, chunk_overlap=60,
    )
    d = m.model_dump(by_alias=True, exclude_none=True)
    assert d["schema"] == 2  # the field is `schema_version`, alias `schema`
    assert d["doc_id"] == "b/o"
    assert "num_chars" not in d["chunk"]  # None fields dropped by exclude_none


def test_deletion_message_parses_wire_payload():
    dm = DeletionMessage.model_validate(
        {"schema": 2, "event": "deletion", "deleted_at": 123,
         "source": {"bucket": "b", "object": "o.pdf", "etag": "e"}}
    )
    assert dm.deleted_at == 123
    assert compute_doc_id(dm.source.bucket, dm.source.object) == "b/o"
