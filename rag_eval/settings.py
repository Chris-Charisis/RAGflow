# Pydantic settings for the Ragas evaluation harness.
from __future__ import annotations
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # Target API under test
    rag_api_url: str = Field("http://rag_api:8000", validation_alias="RAG_API_URL")
    request_timeout: int = Field(120, validation_alias="EVAL_REQUEST_TIMEOUT")

    # Retrieval overrides applied to every eval query (None -> rag_api defaults)
    k: Optional[int] = Field(None, validation_alias="EVAL_K")
    alpha: Optional[float] = Field(None, validation_alias="EVAL_ALPHA")
    rerank: Optional[bool] = Field(None, validation_alias="EVAL_RERANK")
    top_n: Optional[int] = Field(None, validation_alias="EVAL_TOP_N")

    # Dataset / output
    dataset_path: str = Field("/data/questions.jsonl", validation_alias="EVAL_DATASET")
    output_path: str = Field("/data/results.csv", validation_alias="EVAL_OUTPUT")

    # ---- Judge LLM (provider-agnostic, same providers as rag_api) ----
    # Defaults reuse rag_api's LLM_* so you can point both at one model.
    judge_provider: str = Field("ollama", validation_alias="EVAL_LLM_PROVIDER")
    judge_model: Optional[str] = Field(None, validation_alias="EVAL_LLM_MODEL")
    judge_max_tokens: int = Field(1024, validation_alias="EVAL_LLM_MAX_TOKENS")

    # ---- Judge embeddings (for relevancy-type metrics) ----
    embed_provider: str = Field("ollama", validation_alias="EVAL_EMBED_PROVIDER")  # ollama | openai
    embed_model: str = Field("mxbai-embed-large:335m", validation_alias="EVAL_EMBED_MODEL")

    # Provider endpoints / keys (only the chosen provider's are needed)
    ollama_base_url: str = Field("http://ollama:11434", validation_alias="OLLAMA_BASE_URL")
    openai_api_key: Optional[str] = Field(None, validation_alias="OPENAI_API_KEY")
    openai_base_url: Optional[str] = Field(None, validation_alias="OPENAI_BASE_URL")
    google_api_key: Optional[str] = Field(None, validation_alias="GOOGLE_API_KEY")
    anthropic_api_key: Optional[str] = Field(None, validation_alias="ANTHROPIC_API_KEY")

    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()
