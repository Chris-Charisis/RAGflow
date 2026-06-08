# Pydantic settings for the RAG query/answer API service.
# Mirrors the style of the other services (vector_indexer / embedder).
from __future__ import annotations
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # ---------------- Weaviate (must match vector_indexer) ----------------
    weaviate_url: Optional[str] = Field(None, validation_alias="WEAVIATE_URL")
    weaviate_api_key: str = Field("user-a-key", validation_alias="WEAVIATE_API_KEY")
    weaviate_host: str = Field("weaviate", validation_alias="WEAVIATE_HOST")
    weaviate_port: int = Field(8080, validation_alias="WEAVIATE_PORT")
    weaviate_grpc_port: int = Field(50051, validation_alias="WEAVIATE_GRPC_PORT")
    # The collection produced by the ingestion pipeline.
    collection: str = Field("recursive_with_overlap", validation_alias="COLLECTION")
    # Name of the Weaviate property that holds the chunk text (BYO-vector schema).
    text_key: str = Field("text", validation_alias="WEAVIATE_TEXT_KEY")

    # ---------------- Query embeddings (must match embedder) ----------------
    # Same model/endpoint the embedder used at index time, so query vectors live
    # in the same space as the stored document vectors.
    ollama_base_url: str = Field("http://ollama:11434", validation_alias="OLLAMA_BASE_URL")
    embed_model: str = Field("mxbai-embed-large:335m", validation_alias="EMBED_MODEL")
    embed_dimensions: Optional[int] = Field(None, validation_alias="EMBED_DIMENSIONS")
    embed_timeout_seconds: int = Field(60, validation_alias="EMBED_TIMEOUT_SECONDS")
    # mxbai expects this instruction on the QUERY side only. Set empty to disable.
    embed_query_prompt: str = Field(
        "Represent this sentence for searching relevant passages:",
        validation_alias="EMBED_QUERY_PROMPT",
    )

    # ---------------- Retrieval ----------------
    # alpha=1.0 -> pure vector, alpha=0.0 -> pure keyword (BM25). 0.5 is balanced hybrid.
    retrieval_alpha: float = Field(0.5, validation_alias="RETRIEVAL_ALPHA")
    # Candidates fetched from Weaviate before (optional) reranking.
    retrieval_top_k: int = Field(10, validation_alias="RETRIEVAL_TOP_K")

    # ---------------- Reranking (optional, customizable) ----------------
    rerank_enabled: bool = Field(False, validation_alias="RERANK_ENABLED")
    # 'flashrank' (lightweight, default) | 'cross_encoder' | 'cohere'
    rerank_provider: str = Field("flashrank", validation_alias="RERANK_PROVIDER")
    rerank_model: Optional[str] = Field(None, validation_alias="RERANK_MODEL")
    # How many docs survive reranking and are passed to the LLM as context.
    rerank_top_n: int = Field(4, validation_alias="RERANK_TOP_N")
    cohere_api_key: Optional[str] = Field(None, validation_alias="COHERE_API_KEY")

    # ---------------- LLM (provider-agnostic) ----------------
    # 'ollama' (default) | 'openai' | 'gemini' | 'anthropic'
    llm_provider: str = Field("ollama", validation_alias="LLM_PROVIDER")
    llm_model: Optional[str] = Field(None, validation_alias="LLM_MODEL")
    llm_temperature: float = Field(0.0, validation_alias="LLM_TEMPERATURE")
    llm_max_tokens: int = Field(2048, validation_alias="LLM_MAX_TOKENS")

    # Per-provider endpoints / keys (only the selected provider's are required).
    ollama_chat_base_url: str = Field("http://ollama:11434", validation_alias="OLLAMA_CHAT_BASE_URL")
    openai_api_key: Optional[str] = Field(None, validation_alias="OPENAI_API_KEY")
    openai_base_url: Optional[str] = Field(None, validation_alias="OPENAI_BASE_URL")
    google_api_key: Optional[str] = Field(None, validation_alias="GOOGLE_API_KEY")
    anthropic_api_key: Optional[str] = Field(None, validation_alias="ANTHROPIC_API_KEY")

    # ---------------- Server / general ----------------
    api_host: str = Field("0.0.0.0", validation_alias="API_HOST")
    api_port: int = Field(8000, validation_alias="API_PORT")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()
