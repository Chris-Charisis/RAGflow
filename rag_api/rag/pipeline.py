"""RAG pipeline: retrieve (hybrid) -> rerank (optional) -> prompt -> LLM.

Built on LangChain primitives (Documents, prompt templates, chat models, LCEL
streaming) but keeps retrieval/rerank explicit so the API can return grounded
sources alongside the answer.
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Iterator, List, Optional

from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from ..clients.embeddings import QueryEmbedder
from ..clients.weaviate_client import connect_weaviate
from ..settings import Settings
from .llm import build_llm
from .reranker import build_reranker
from .retriever import WeaviateHybridRetriever

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a precise research assistant. Answer the user's question using ONLY "
    "the provided context passages. If the context does not contain the answer, "
    "say you don't know rather than guessing. Cite the passages you used with "
    "their bracketed numbers, e.g. [1], [2]."
)

HUMAN_PROMPT = "Context:\n{context}\n\nQuestion: {question}\n\nAnswer:"


class RAGPipeline:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.client = connect_weaviate(cfg)
        self.collection = self.client.collections.get(cfg.collection)
        self.embedder = QueryEmbedder(
            base_url=cfg.ollama_base_url,
            model=cfg.embed_model,
            query_prompt=cfg.embed_query_prompt,
            dimensions=cfg.embed_dimensions,
            timeout_s=cfg.embed_timeout_seconds,
        )
        # Built on first use (loading a reranker model can be expensive); cached.
        self._reranker = None
        self.llm = build_llm(cfg)
        self.prompt = ChatPromptTemplate.from_messages(
            [("system", SYSTEM_PROMPT), ("human", HUMAN_PROMPT)]
        )
        self.chain = self.prompt | self.llm | StrOutputParser()

    # ---------------- retrieval ----------------
    def retrieve(
        self, question: str, *, k: Optional[int] = None, alpha: Optional[float] = None,
        rerank: Optional[bool] = None, top_n: Optional[int] = None,
    ) -> List[Document]:
        retriever = WeaviateHybridRetriever(
            collection=self.collection,
            embedder=self.embedder,
            text_key=self.cfg.text_key,
            alpha=self.cfg.retrieval_alpha if alpha is None else alpha,
            k=self.cfg.retrieval_top_k if k is None else k,
        )
        docs = retriever.invoke(question)

        do_rerank = self.cfg.rerank_enabled if rerank is None else rerank
        if do_rerank and docs:
            docs = self._get_reranker().rerank(
                question, docs, top_n or self.cfg.rerank_top_n
            )
        return docs

    def _get_reranker(self):
        if self._reranker is None:
            self._reranker = build_reranker(self.cfg, force=True)
        return self._reranker

    # ---------------- prompting helpers ----------------
    @staticmethod
    def _format_context(docs: List[Document]) -> str:
        blocks = []
        for i, d in enumerate(docs, start=1):
            meta = d.metadata or {}
            title = meta.get("title") or ""
            header = f"[{i}]" + (f" {title}" if title else "")
            blocks.append(f"{header}\n{d.page_content}")
        return "\n\n".join(blocks)

    @staticmethod
    def _sources(docs: List[Document]) -> List[Dict[str, Any]]:
        # Flat schema (vector_indexer v2): identifiers live at the top level.
        out = []
        for i, d in enumerate(docs, start=1):
            meta = d.metadata or {}
            out.append(
                {
                    "n": i,
                    "title": meta.get("title"),
                    "doc_id": meta.get("doc_id"),
                    "object": meta.get("object"),
                    "chunk_index": meta.get("chunk_index"),
                    "score": meta.get("rerank_score", meta.get("score")),
                    "snippet": (d.page_content or "")[:300],
                }
            )
        return out

    # ---------------- public API ----------------
    def answer(self, question: str, **kwargs) -> Dict[str, Any]:
        docs = self.retrieve(question, **kwargs)
        if not docs:
            return {"answer": "I couldn't find anything relevant in the knowledge base.", "sources": []}
        answer = self.chain.invoke(
            {"context": self._format_context(docs), "question": question}
        )
        return {"answer": answer, "sources": self._sources(docs)}

    def stream(self, question: str, **kwargs) -> Iterator[Dict[str, Any]]:
        """Yield {'type': 'token'|'sources', 'data': ...} events."""
        docs = self.retrieve(question, **kwargs)
        if not docs:
            yield {"type": "token", "data": "I couldn't find anything relevant in the knowledge base."}
            yield {"type": "sources", "data": []}
            return
        for token in self.chain.stream(
            {"context": self._format_context(docs), "question": question}
        ):
            yield {"type": "token", "data": token}
        yield {"type": "sources", "data": self._sources(docs)}

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
