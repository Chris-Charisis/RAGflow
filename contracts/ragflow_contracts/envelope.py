"""RAGflow message contract — the single source of truth for the event envelope
that flows pdf_reader -> text_chunker -> embedder -> vector_indexer.

Every service imports these models / helpers instead of hand-rolling dict shapes,
so producers and consumers cannot drift (this is what previously let the
`chunk` vs `metadata.chunk` mismatch slip through). Models use extra="allow" so
forward-compatible fields pass through untouched.

Key shared invariants:
  * compute_doc_id() — MUST be identical in the producer (chunker) and the
    deletion consumer (vector_indexer) so deletions target the right objects.
  * chunk_uuid()     — deterministic per (doc_id, chunk_index) so re-indexing
    upserts in place instead of creating duplicates.
  * published_at / deleted_at — millisecond timestamps used to resolve the
    "deleted while still being processed" race (see DeletionMessage).
"""
from __future__ import annotations

import time
import uuid
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# Bump when the envelope changes shape in a breaking way.
SCHEMA_VERSION = 2

# Stable namespace so chunk UUIDs are reproducible across processes and restarts.
NAMESPACE = uuid.UUID("6f9c3e2a-5b1d-4f8a-9c0e-2a7b6d4e1f33")


def now_ms() -> int:
    """Current UTC time in milliseconds (monotonic enough for ordering events)."""
    return int(time.time() * 1000)


def compute_doc_id(bucket: Optional[str], object_name: Optional[str]) -> str:
    """Stable document id derived from the source location.

    Strips a trailing '.pdf'. MUST stay identical between the chunker (which
    stamps it onto every chunk) and the vector_indexer deletion handler (which
    recomputes it from the deletion event's source).
    """
    if not object_name:
        return ""
    oid = object_name[:-4] if object_name.lower().endswith(".pdf") else object_name
    return f"{bucket or ''}/{oid}"


def chunk_uuid(doc_id: str, chunk_index: int) -> str:
    """Deterministic UUID for a chunk -> enables idempotent upserts."""
    return str(uuid.uuid5(NAMESPACE, f"{doc_id}:{chunk_index}"))


def tombstone_uuid(doc_id: str) -> str:
    """Deterministic UUID for a document's deletion tombstone."""
    return str(uuid.uuid5(NAMESPACE, f"tombstone:{doc_id}"))


class Source(BaseModel):
    model_config = ConfigDict(extra="allow")
    bucket: Optional[str] = None
    object: Optional[str] = None
    etag: Optional[str] = None


class Metadata(BaseModel):
    model_config = ConfigDict(extra="allow")
    title: Optional[str] = None
    authors: Optional[Any] = None
    keywords: Optional[Any] = None
    abstract: Optional[str] = None
    doi: Optional[str] = None


class Chunk(BaseModel):
    model_config = ConfigDict(extra="allow")
    index: int
    text: str
    # Provenance / sizing (populated by the token-aware chunker).
    start: Optional[int] = None  # char offset (inclusive) in the chunked source text
    end: Optional[int] = None  # char offset (exclusive)
    num_chars: Optional[int] = None
    num_words: Optional[int] = None
    num_tokens: Optional[int] = None
    chapter: Optional[str] = None


class IngestMessage(BaseModel):
    """pdf_reader -> text_chunker (one per document)."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    schema_version: int = Field(SCHEMA_VERSION, alias="schema")
    event: str = "ingest"
    published_at: int = Field(default_factory=now_ms)
    source: Source
    metadata: Metadata = Field(default_factory=Metadata)
    text: str = ""


class ChunkMessage(BaseModel):
    """text_chunker -> embedder -> vector_indexer (one per chunk)."""
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    schema_version: int = Field(SCHEMA_VERSION, alias="schema")
    event: str = "chunk"
    published_at: int = Field(default_factory=now_ms)
    source: Source
    metadata: Metadata = Field(default_factory=Metadata)
    doc_id: str
    chunk: Chunk
    # Eval / reproducibility markers (used by Ragas-style evaluation harnesses).
    chunk_strategy: Optional[str] = None
    chunk_size: Optional[int] = None
    chunk_overlap: Optional[int] = None
    # Filled in by the embedder; kept loose to avoid coupling.
    embedding: Optional[dict] = None


class DeletionMessage(BaseModel):
    """pdf_reader -> vector_indexer: source object no longer exists.

    `deleted_at` lets the indexer reject chunks of the deleted generation that
    are still in flight (published_at <= deleted_at) while preserving any newer
    re-ingestion (published_at > deleted_at).
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    schema_version: int = Field(SCHEMA_VERSION, alias="schema")
    event: str = "deletion"
    deleted_at: int = Field(default_factory=now_ms)
    source: Source
    doc_id: Optional[str] = None


__all__ = [
    "SCHEMA_VERSION",
    "NAMESPACE",
    "now_ms",
    "compute_doc_id",
    "chunk_uuid",
    "tombstone_uuid",
    "Source",
    "Metadata",
    "Chunk",
    "IngestMessage",
    "ChunkMessage",
    "DeletionMessage",
]
