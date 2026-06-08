"""Weaviate v4 connection helper for the read path.

Connects to the same collection the vector_indexer writes to. The collection
uses BYO ("self_provided") vectors, so at query time we must supply the query
vector ourselves — see rag/retriever.py.
"""
from __future__ import annotations
import logging

import weaviate
from weaviate.classes.init import Auth

from ..settings import Settings

logger = logging.getLogger(__name__)


def connect_weaviate(cfg: Settings):
    if cfg.weaviate_url:
        auth = Auth.api_key(cfg.weaviate_api_key) if cfg.weaviate_api_key else None
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=cfg.weaviate_url,
            auth_credentials=auth,
        )
    else:
        client = weaviate.connect_to_local(
            host=cfg.weaviate_host,
            port=cfg.weaviate_port,
            grpc_port=cfg.weaviate_grpc_port,
            auth_credentials=Auth.api_key(cfg.weaviate_api_key)
            if cfg.weaviate_api_key
            else None,
        )
    logger.info("Connected to Weaviate (is_ready=%s)", client.is_ready())
    return client
