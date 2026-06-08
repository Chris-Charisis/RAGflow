"""FastAPI app exposing the RAG query/answer endpoints.

Endpoints:
  GET  /health        - liveness + Weaviate readiness
  POST /query         - non-streaming: {answer, sources}
  POST /chat/stream   - NDJSON stream of {type: token|sources, data: ...}
"""
from __future__ import annotations
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.responses import Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

from .rag.pipeline import RAGPipeline
from .settings import settings

logger = logging.getLogger(__name__)

_queries = Counter("rag_api_queries_total", "Queries handled", ["endpoint"])
_latency = Histogram("rag_api_query_seconds", "Query latency (seconds)", ["endpoint"])


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1)
    k: Optional[int] = None
    alpha: Optional[float] = None
    rerank: Optional[bool] = None
    top_n: Optional[int] = None
    # When true, include the full retrieved context strings (for evaluation).
    return_contexts: bool = False


class Source(BaseModel):
    n: int
    title: Optional[str] = None
    doc_id: Optional[str] = None
    object: Optional[str] = None
    chunk_index: Optional[int] = None
    score: Optional[float] = None
    snippet: Optional[str] = None


class QueryResponse(BaseModel):
    answer: str
    sources: List[Source]
    contexts: Optional[List[str]] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pipeline = RAGPipeline(settings)
    logger.info("RAG pipeline ready")
    try:
        yield
    finally:
        app.state.pipeline.close()


app = FastAPI(title="RAGflow Query API", version="2.0", lifespan=lifespan)


@app.get("/health")
def health() -> Dict[str, Any]:
    pipeline: RAGPipeline = app.state.pipeline
    ready = False
    try:
        ready = pipeline.client.is_ready()
    except Exception as e:  # pragma: no cover
        logger.warning("Weaviate readiness check failed: %s", e)
    return {"status": "ok", "weaviate_ready": ready, "llm_provider": settings.llm_provider}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest) -> Dict[str, Any]:
    pipeline: RAGPipeline = app.state.pipeline
    _queries.labels("query").inc()
    with _latency.labels("query").time():
        result = pipeline.answer(
            req.question, k=req.k, alpha=req.alpha, rerank=req.rerank, top_n=req.top_n
        )
    if not req.return_contexts:
        result.pop("contexts", None)
    return result


@app.post("/chat/stream")
def chat_stream(req: QueryRequest) -> StreamingResponse:
    pipeline: RAGPipeline = app.state.pipeline

    def gen():
        for event in pipeline.stream(
            req.question, k=req.k, alpha=req.alpha, rerank=req.rerank, top_n=req.top_n
        ):
            yield json.dumps(event) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")
