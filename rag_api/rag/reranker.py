"""Optional, pluggable reranking stage.

Reranking is OFF by default (settings.rerank_enabled). When on, a cross-encoder
style reranker scores each retrieved chunk against the query and keeps the top-N.
Three backends are supported; each is imported lazily so its (sometimes heavy)
dependency is only required when actually selected:

  - flashrank      : lightweight ONNX cross-encoder, no torch. Good default.
  - cross_encoder  : sentence-transformers CrossEncoder (pulls torch).
  - cohere         : Cohere Rerank API (needs COHERE_API_KEY).

Add a new backend by implementing `Reranker.rerank` and extending `build_reranker`.
"""
from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from langchain_core.documents import Document

from ..settings import Settings

logger = logging.getLogger(__name__)


class Reranker(ABC):
    @abstractmethod
    def rerank(self, query: str, docs: List[Document], top_n: int) -> List[Document]:
        ...


class NoOpReranker(Reranker):
    def rerank(self, query: str, docs: List[Document], top_n: int) -> List[Document]:
        return docs[:top_n]


class FlashrankReranker(Reranker):
    def __init__(self, model: Optional[str] = None):
        try:
            from flashrank import Ranker, RerankRequest
        except ImportError as e:
            raise ImportError(
                "flashrank is not installed. `pip install flashrank` or pick a "
                "different RERANK_PROVIDER."
            ) from e
        self._RerankRequest = RerankRequest
        self.ranker = Ranker(model_name=model) if model else Ranker()

    def rerank(self, query: str, docs: List[Document], top_n: int) -> List[Document]:
        passages = [{"id": i, "text": d.page_content} for i, d in enumerate(docs)]
        ranked = self.ranker.rerank(self._RerankRequest(query=query, passages=passages))
        out: List[Document] = []
        for item in ranked[:top_n]:
            doc = docs[item["id"]]
            doc.metadata["rerank_score"] = float(item.get("score", 0.0))
            out.append(doc)
        return out


class CrossEncoderReranker(Reranker):
    def __init__(self, model: Optional[str] = None):
        try:
            from sentence_transformers import CrossEncoder
        except ImportError as e:
            raise ImportError(
                "sentence-transformers is not installed. `pip install sentence-transformers` "
                "or pick a different RERANK_PROVIDER."
            ) from e
        self.model = CrossEncoder(model or "cross-encoder/ms-marco-MiniLM-L-6-v2")

    def rerank(self, query: str, docs: List[Document], top_n: int) -> List[Document]:
        scores = self.model.predict([(query, d.page_content) for d in docs])
        ranked = sorted(zip(docs, scores), key=lambda x: x[1], reverse=True)
        out: List[Document] = []
        for doc, score in ranked[:top_n]:
            doc.metadata["rerank_score"] = float(score)
            out.append(doc)
        return out


class CohereReranker(Reranker):
    def __init__(self, api_key: Optional[str], model: Optional[str] = None):
        if not api_key:
            raise ValueError("COHERE_API_KEY is required for the 'cohere' reranker.")
        try:
            import cohere
        except ImportError as e:
            raise ImportError(
                "cohere is not installed. `pip install cohere` or pick a different "
                "RERANK_PROVIDER."
            ) from e
        self.client = cohere.Client(api_key)
        self.model = model or "rerank-english-v3.0"

    def rerank(self, query: str, docs: List[Document], top_n: int) -> List[Document]:
        resp = self.client.rerank(
            query=query,
            documents=[d.page_content for d in docs],
            top_n=min(top_n, len(docs)),
            model=self.model,
        )
        out: List[Document] = []
        for r in resp.results:
            doc = docs[r.index]
            doc.metadata["rerank_score"] = float(r.relevance_score)
            out.append(doc)
        return out


def build_reranker(cfg: Settings, *, force: bool = False) -> Reranker:
    """Build the configured reranker.

    `force=True` builds the concrete provider even when rerank_enabled is False
    (used for per-request `rerank=true` overrides). Otherwise the global
    rerank_enabled flag decides whether to return a no-op.
    """
    if not (force or cfg.rerank_enabled):
        return NoOpReranker()
    provider = (cfg.rerank_provider or "flashrank").strip().lower()
    logger.info("Building reranker (provider=%s, top_n=%s)", provider, cfg.rerank_top_n)
    if provider == "flashrank":
        return FlashrankReranker(cfg.rerank_model)
    if provider == "cross_encoder":
        return CrossEncoderReranker(cfg.rerank_model)
    if provider == "cohere":
        return CohereReranker(cfg.cohere_api_key, cfg.rerank_model)
    raise ValueError(f"Unknown rerank provider: {cfg.rerank_provider}")
