from __future__ import annotations
import logging
from abc import ABC, abstractmethod

from ragflow_contracts import chunk_uuid, now_ms, tombstone_uuid

from ..settings import Settings
from ..helpers import (
    extract_vector,
    retry,
    _clean_text,
    _to_text_array,
    _to_int,
    _drop_nones,
)

logger = logging.getLogger(__name__)

# ---------- Base (DB-agnostic) ----------


class BaseIndexBackend(ABC):
    def __init__(self, cfg: Settings):
        self.cfg = cfg

    @abstractmethod
    def connect(self) -> None: ...
    @abstractmethod
    def ensure_ready(self) -> None: ...
    @abstractmethod
    def upsert_one(self, payload: dict) -> None: ...
    @abstractmethod
    def delete_document(self, doc_id: str, deleted_at: int) -> int: ...
    @abstractmethod
    def close(self) -> None: ...


# ---------- Weaviate (v4) implementation ----------


class WeaviateIndexBackend(BaseIndexBackend):
    """BYO-vectors Weaviate backend with idempotent upserts and tombstoned deletes.

    Schema is FLAT (no nested objects) so identifiers and offsets are filterable
    — required for delete-by-filter and for evaluation/citation queries.

    Deletion race handling: a deletion writes a tombstone (doc_id -> deleted_at)
    and removes chunks with published_at <= deleted_at. Any chunk that arrives
    afterwards is checked against the tombstone and skipped if it belongs to the
    deleted generation, while a newer re-ingestion (published_at > deleted_at)
    indexes normally.
    """

    def __init__(self, cfg: Settings):
        super().__init__(cfg)
        self.client = None
        self.collection = None
        self.tombstones = None

    def connect(self) -> None:
        import weaviate
        from weaviate.classes.init import Auth

        if self.cfg.weaviate_url:
            auth = Auth.api_key(self.cfg.weaviate_api_key) if self.cfg.weaviate_api_key else None
            self.client = weaviate.connect_to_weaviate_cloud(
                cluster_url=self.cfg.weaviate_url,
                auth_credentials=auth,
            )
        else:
            self.client = weaviate.connect_to_local(
                host=self.cfg.weaviate_host,
                port=self.cfg.weaviate_port,
                grpc_port=self.cfg.weaviate_grpc_port,
                auth_credentials=Auth.api_key(self.cfg.weaviate_api_key)
                if self.cfg.weaviate_api_key
                else None,
            )
        logger.info("Connected to Weaviate (is_ready=%s)", self.client.is_ready())

    def ensure_ready(self) -> None:
        from weaviate.classes.config import Configure, DataType, Property, Tokenization

        # ----- Main chunk collection -----
        name = self.cfg.collection
        if self.client.collections.exists(name):
            self.collection = self.client.collections.get(name)
        elif self.cfg.create_collection_if_missing:
            logger.info("Creating collection '%s' (vectors: self_provided)", name)
            self.collection = self.client.collections.create(
                name,
                vector_config=Configure.Vectors.self_provided(),
                properties=[
                    # Searchable text
                    Property(name="text", data_type=DataType.TEXT,
                             tokenization=Tokenization.WORD, index_searchable=True),
                    Property(name="title", data_type=DataType.TEXT,
                             tokenization=Tokenization.LOWERCASE, index_searchable=True),
                    Property(name="keywords", data_type=DataType.TEXT_ARRAY,
                             tokenization=Tokenization.WORD, index_searchable=True, index_filterable=True),
                    Property(name="authors", data_type=DataType.TEXT_ARRAY,
                             tokenization=Tokenization.LOWERCASE, index_searchable=True, index_filterable=True),
                    # Filterable identifiers (FIELD = exact match) — used for deletion & eval
                    Property(name="doc_id", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="object", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="bucket", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="etag", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="doi", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    # Ordering / race resolution
                    Property(name="published_at", data_type=DataType.INT,
                             index_filterable=True, index_range_filters=True),
                    # Chunk provenance / offsets (citation + eval)
                    Property(name="chunk_index", data_type=DataType.INT,
                             index_filterable=True, index_range_filters=True),
                    Property(name="char_start", data_type=DataType.INT, index_range_filters=True),
                    Property(name="char_end", data_type=DataType.INT, index_range_filters=True),
                    Property(name="num_chars", data_type=DataType.INT, index_range_filters=True),
                    Property(name="num_tokens", data_type=DataType.INT, index_range_filters=True),
                    Property(name="chapter", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    # Eval / reproducibility markers (Ragas-style harnesses)
                    Property(name="chunk_strategy", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="chunk_size", data_type=DataType.INT, index_filterable=True),
                    Property(name="chunk_overlap", data_type=DataType.INT, index_filterable=True),
                    Property(name="embedding_model", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="ingested_at", data_type=DataType.INT, index_range_filters=True),
                ],
            )
        else:
            raise RuntimeError(f"Collection '{name}' missing and create disabled")

        # ----- Tombstone collection -----
        ts_name = self.cfg.tombstone_collection
        if self.client.collections.exists(ts_name):
            self.tombstones = self.client.collections.get(ts_name)
        else:
            logger.info("Creating tombstone collection '%s'", ts_name)
            self.tombstones = self.client.collections.create(
                ts_name,
                vector_config=Configure.Vectors.self_provided(),
                properties=[
                    Property(name="doc_id", data_type=DataType.TEXT,
                             tokenization=Tokenization.FIELD, index_filterable=True),
                    Property(name="deleted_at", data_type=DataType.INT,
                             index_filterable=True, index_range_filters=True),
                ],
            )

    # ----- payload -> flat Weaviate object -----
    def to_weaviate_object(self, d: dict) -> dict:
        chunk = d.get("chunk") or {}
        meta = d.get("metadata") or {}
        src = d.get("source") or {}
        emb = d.get("embedding") or {}

        obj = {
            "text": _clean_text(chunk.get("text")),
            "title": _clean_text(meta.get("title")),
            "keywords": _to_text_array(meta.get("keywords")),
            "authors": _to_text_array(meta.get("authors")),
            "doi": _clean_text(meta.get("doi")),
            "doc_id": _clean_text(d.get("doc_id")),
            "object": _clean_text(src.get("object")),
            "bucket": _clean_text(src.get("bucket")),
            "etag": _clean_text(src.get("etag")),
            "published_at": _to_int(d.get("published_at")),
            "chunk_index": _to_int(chunk.get("index")),
            "char_start": _to_int(chunk.get("start")),
            "char_end": _to_int(chunk.get("end")),
            "num_chars": _to_int(chunk.get("num_chars")),
            "num_tokens": _to_int(chunk.get("num_tokens")),
            "chapter": _clean_text(chunk.get("chapter")),
            # eval / repro markers
            "chunk_strategy": _clean_text(d.get("chunk_strategy")),
            "chunk_size": _to_int(d.get("chunk_size")),
            "chunk_overlap": _to_int(d.get("chunk_overlap")),
            "embedding_model": _clean_text(emb.get("embedding_model")),
            "ingested_at": now_ms(),
        }
        return _drop_nones(obj)

    # ----- tombstone helpers -----
    def _tombstone_deleted_at(self, doc_id: str):
        """Return the deleted_at timestamp for doc_id, or None if no tombstone."""
        obj = self.tombstones.query.fetch_object_by_id(tombstone_uuid(doc_id))
        if obj is None:
            return None
        return _to_int((obj.properties or {}).get("deleted_at"))

    def _write_tombstone(self, doc_id: str, deleted_at: int) -> None:
        uid = tombstone_uuid(doc_id)
        existing = self.tombstones.query.fetch_object_by_id(uid)
        props = {"doc_id": doc_id, "deleted_at": deleted_at}
        if existing is None:
            self.tombstones.data.insert(properties=props, uuid=uid)
        else:
            # Keep the latest deletion instant.
            prev = _to_int((existing.properties or {}).get("deleted_at")) or 0
            props["deleted_at"] = max(prev, deleted_at)
            self.tombstones.data.replace(uuid=uid, properties=props)

    # ----- write path -----
    @retry((Exception,), tries=3, delay=0.3, backoff=2.0)
    def upsert_one(self, payload: dict) -> None:
        doc_id = _clean_text(payload.get("doc_id"))
        published_at = _to_int(payload.get("published_at"))
        chunk_index = _to_int((payload.get("chunk") or {}).get("index"))

        # Reject chunks belonging to an already-deleted generation (race guard).
        if doc_id and published_at is not None:
            tomb = self._tombstone_deleted_at(doc_id)
            if tomb is not None and published_at <= tomb:
                logger.info(
                    "Skipping chunk for deleted doc_id=%s (published_at=%s <= deleted_at=%s)",
                    doc_id, published_at, tomb,
                )
                return

        weaviate_object = self.to_weaviate_object(payload)
        vec = extract_vector(payload)

        if self.cfg.dry_run:
            logger.info("[DRY RUN] insert dim=%s doc_id=%s idx=%s", len(vec), doc_id, chunk_index)
            return

        # Deterministic UUID -> idempotent upsert (no duplicates on re-index).
        if doc_id and chunk_index is not None:
            uid = chunk_uuid(doc_id, chunk_index)
            if self.collection.data.exists(uid):
                self.collection.data.replace(uuid=uid, properties=weaviate_object, vector=vec)
            else:
                self.collection.data.insert(properties=weaviate_object, vector=vec, uuid=uid)
        else:
            # Fall back to a server-assigned id when we can't build a stable one.
            self.collection.data.insert(weaviate_object, vector=vec)

    # ----- delete path -----
    @retry((Exception,), tries=3, delay=0.3, backoff=2.0)
    def delete_document(self, doc_id: str, deleted_at: int) -> int:
        from weaviate.classes.query import Filter

        # 1) Tombstone first, so in-flight chunks are rejected even if delete races.
        self._write_tombstone(doc_id, deleted_at)

        # 2) Remove chunks of the deleted generation (published at/before deletion).
        #    A newer re-ingestion (published_at > deleted_at) is preserved.
        where = Filter.by_property("doc_id").equal(doc_id) & Filter.by_property(
            "published_at"
        ).less_or_equal(deleted_at)
        if self.cfg.dry_run:
            logger.info("[DRY RUN] would delete chunks for doc_id=%s <= %s", doc_id, deleted_at)
            return 0
        result = self.collection.data.delete_many(where=where)
        return int(getattr(result, "successful", 0) or 0)

    def close(self) -> None:
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass


# ---------- Orchestrator (DB-agnostic API) ----------


class DatabaseIndexer:
    """Thin wrapper to select and drive a backend."""

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.backend: BaseIndexBackend = self._factory(cfg.backend)

    def _factory(self, name: str) -> BaseIndexBackend:
        key = (name or "weaviate").strip().lower()
        if key == "weaviate":
            return WeaviateIndexBackend(self.cfg)
        raise ValueError(f"Unknown index backend: {name}")

    def start(self) -> None:
        self.backend.connect()
        self.backend.ensure_ready()

    def upsert(self, payload: dict) -> None:
        self.backend.upsert_one(payload)

    def delete(self, doc_id: str, deleted_at: int) -> int:
        return self.backend.delete_document(doc_id, deleted_at)

    def stop(self) -> None:
        self.backend.close()
