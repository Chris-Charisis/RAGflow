from __future__ import annotations
import logging
from abc import ABC, abstractmethod
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
    def close(self) -> None: ...

# ---------- Weaviate (v4) implementation ----------

class WeaviateIndexBackend(BaseIndexBackend):
    """
    BYO vectors backend for Weaviate v4:
      - Uses connect_to_local / connect_to_weaviate_cloud helpers
      - Creates a collection configured with self_provided vectors
      - Inserts objects with vector=...
    Docs: helpers and BYO vectors.  # see README for links
    """
    def __init__(self, cfg: Settings):
        super().__init__(cfg)
        self.client = None
        self.collection = None

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
                auth_credentials=Auth.api_key(self.cfg.weaviate_api_key) if self.cfg.weaviate_api_key else None,

            )
        logger.info("Connected to Weaviate (is_ready=%s)", self.client.is_ready())

    def ensure_ready(self) -> None:
        from weaviate.classes.config import Property, DataType, Configure, Tokenization
        name = self.cfg.collection
        try:
            self.collection = self.client.collections.get(name)
            return
        except Exception:
            if not self.cfg.create_collection_if_missing:
                raise

        # Setup collection's schema
        logger.info("Creating collection '%s' (vectors: self_provided)", name)
        self.collection = self.client.collections.create(
            name,
            vector_config=Configure.Vectors.self_provided(),
            properties=[
                # --- Searchable text ----
                Property(
                    name="text",
                    data_type=DataType.TEXT,
                    tokenization=Tokenization.WORD,
                    index_searchable=True,          # BM25 on this field
                ),
                Property(
                    name="title",
                    data_type=DataType.TEXT,
                    tokenization=Tokenization.LOWERCASE,
                    index_searchable=True,
                ),
                Property(
                    name="keywords",
                    data_type=DataType.TEXT_ARRAY,
                    tokenization=Tokenization.WORD,
                    index_searchable=True,
                    index_filterable=True,          # allow where filters like keywords contains "agriculture"
                ),
                Property(
                    name="authors",
                    data_type=DataType.TEXT_ARRAY,
                    tokenization=Tokenization.LOWERCASE,
                    index_searchable=True,
                    index_filterable=True,          # allow where filters like authors contains "Bob"
                ),

                # --- Filterable identifiers / ranges ----
                # Property(
                #     name="doc_id",
                #     data_type=DataType.TEXT,
                #     tokenization=Tokenization.FIELD,  # keep exact value; good for equality filters
                #     index_filterable=True,
                # ),
               
                # Property(name="chunk_index",data_type=DataType.INT, index_filterable=True, index_range_filters=True),
                # Property(name="char_start", data_type=DataType.INT, index_range_filters=True),
                # Property(name="char_end", data_type=DataType.INT, index_range_filters=True),
                # Property(name="num_chars", data_type=DataType.INT, index_range_filters=True),
                # --- Original nested blobs for convenient retrieval (NOT searchable) ---
                # Object / nested properties are stored but not indexed or vectorized.
                Property(
                    name="metadata",
                    data_type=DataType.OBJECT,
                    nested_properties=[
                        Property(name="title",    data_type=DataType.TEXT,        tokenization=Tokenization.LOWERCASE),
                        Property(name="authors",  data_type=DataType.TEXT_ARRAY,  tokenization=Tokenization.LOWERCASE),
                        Property(name="keywords", data_type=DataType.TEXT_ARRAY,  tokenization=Tokenization.WORD),
                        # Property(name="abstract", data_type=DataType.TEXT,        tokenization=Tokenization.WORD),
                        Property(name="doi",      data_type=DataType.TEXT,        tokenization=Tokenization.FIELD),
                        Property(name="doc_id", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
                        Property(name="bucket", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
                        Property(name="object", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
                        Property(name="etag",   data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
                        Property(name="schema_version", data_type=DataType.INT, tokenization=Tokenization.FIELD),
                    ],
                ),
                Property(
                    name="chunk",
                    data_type=DataType.OBJECT,
                    nested_properties=[
                        Property(name="index",     data_type=DataType.INT),
                        Property(name="start",     data_type=DataType.INT),
                        Property(name="end",       data_type=DataType.INT),
                        Property(name="num_chars", data_type=DataType.INT),
                        Property(name="text",      data_type=DataType.TEXT, tokenization=Tokenization.WORD),
                    ],
                ),
            ],
        )

    # Convert the standard payload dict into Weaviate object properties
    def to_weaviate_object(self, d: dict) -> dict:
        chunk = d.get("chunk") or {}
        meta  = d.get("metadata") or {}
        src   = d.get("source") or {}

        weaviate_object = {
            # searchable / filterable
            "text": _clean_text(chunk.get("text")),
            "title": _clean_text(meta.get("title")),
            "keywords": _to_text_array(meta.get("keywords")),
            "authors": _to_text_array(meta.get("authors")),
            # original nested blobs (for retrieval)
            "metadata": {
                "title": _clean_text(meta.get("title")),
                "authors": _to_text_array(meta.get("authors")),
                "keywords": _to_text_array(meta.get("keywords")),
                "doc_id": _clean_text(d.get("doc_id")),
                # "abstract": _clean_text(meta.get("abstract")),
                "doi": _clean_text(meta.get("doi")),
                "bucket": _clean_text(src.get("bucket")),
                "object": _clean_text(src.get("object")),
                "etag": _clean_text(src.get("etag")),
                "schema_version": _to_int(d.get("schema", 1)),            
                "chunk": {
                    "index": _to_int(chunk.get("index")),
                    "start": _to_int(chunk.get("start")),
                    "end": _to_int(chunk.get("end")),
                    "num_chars": _to_int(chunk.get("num_chars")),
                },
            },
        }

        return _drop_nones(weaviate_object)

    @retry((Exception,), tries=3, delay=0.3, backoff=2.0)
    def upsert_one(self, payload: dict) -> None:
        # props = dict_pick(payload, ["schema"])
        weaviate_object = self.to_weaviate_object(payload)
        vec = extract_vector(payload)
        # print(vec)
        # uid = ensure_uuid(payload.get("doc_id"))

        if self.cfg.dry_run:
            logger.info("[DRY RUN] Weaviate insert dim=%s props_keys=%s",
                        len(vec), list(weaviate_object.keys()))
            return

        self.collection.data.insert(
            weaviate_object,
            vector=vec,
            # uuid=uid,
        )

    def close(self) -> None:
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass

# ---------- Orchestrator (DB-agnostic API) ----------

class DatabaseIndexer:
    """
    Thin wrapper to select and drive a backend.
    Add new DBs by implementing BaseIndexBackend and extending _factory().
    """
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.backend: BaseIndexBackend = self._factory(cfg.backend)

    def _factory(self, name: str) -> BaseIndexBackend:
        key = (name or "weaviate").strip().lower()
        if key == "weaviate":
            return WeaviateIndexBackend(self.cfg)
        # elif key == "opensearch": return OpenSearchIndexBackend(self.cfg)
        # elif key == "pgvector":  return PGVectorIndexBackend(self.cfg)
        raise ValueError(f"Unknown index backend: {name}")

    def start(self) -> None:
        self.backend.connect()
        self.backend.ensure_ready()

    def upsert(self, payload: dict) -> None:
        self.backend.upsert_one(payload)

    def stop(self) -> None:
        self.backend.close()
