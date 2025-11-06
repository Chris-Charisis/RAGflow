from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any

from ..settings import Settings
from ..helpers import dict_pick, extract_vector, ensure_uuid, retry

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
        from weaviate.classes.config import Property, DataType, Configure
        name = self.cfg.collection
        try:
            self.collection = self.client.collections.get(name)
            return
        except Exception:
            if not self.cfg.create_collection_if_missing:
                raise

        logger.info("Creating collection '%s' (BYO vectors: self_provided)", name)
        self.collection = self.client.collections.create(
            name,
            vector_config=Configure.Vectors.self_provided(),
            properties=[
                Property(name="schema", data_type=DataType.TEXT),
            ],
        )

    @retry((Exception,), tries=3, delay=0.3, backoff=2.0)
    def upsert_one(self, payload: dict) -> None:
        props = dict_pick(payload, ["schema"])
        vec = extract_vector(payload)
        # print(vec)
        # uid = ensure_uuid(payload.get("doc_id"))

        if self.cfg.dry_run:
            logger.info("[DRY RUN] Weaviate insert dim=%s props_keys=%s",
                        len(vec), list(props.keys()))
            return

        self.collection.data.insert(
            properties=props,
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
