"""Hybrid (BM25 + vector) retriever over a BYO-vector Weaviate collection.

LangChain's stock WeaviateVectorStore assumes Weaviate vectorizes text for you.
Our collection stores self-provided vectors, so we embed the query ourselves and
drive the native v4 `collection.query.hybrid(...)` call, wrapping the result as a
LangChain `BaseRetriever` so it composes with the rest of the LCEL pipeline.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List

from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict
from weaviate.classes.query import MetadataQuery

from ..clients.embeddings import QueryEmbedder

logger = logging.getLogger(__name__)


class WeaviateHybridRetriever(BaseRetriever):
    """Retrieve chunks via Weaviate hybrid search using a self-provided query vector."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    collection: Any
    embedder: QueryEmbedder
    text_key: str = "text"
    alpha: float = 0.5
    k: int = 10

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        vector = self.embedder.embed_query(query)
        result = self.collection.query.hybrid(
            query=query,
            vector=vector,
            alpha=self.alpha,
            limit=self.k,
            return_metadata=MetadataQuery(score=True),
        )
        docs: List[Document] = []
        for obj in result.objects:
            props: Dict[str, Any] = obj.properties or {}
            text = props.get(self.text_key) or ""
            meta: Dict[str, Any] = {k: v for k, v in props.items() if k != self.text_key}
            meta["uuid"] = str(obj.uuid)
            if obj.metadata is not None and obj.metadata.score is not None:
                meta["score"] = obj.metadata.score
            docs.append(Document(page_content=text, metadata=meta))
        logger.debug("Hybrid retrieval (alpha=%s, k=%s) -> %d docs", self.alpha, self.k, len(docs))
        return docs
