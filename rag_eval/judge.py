"""Judge model + embeddings factories for Ragas (provider-agnostic).

Mirrors rag_api's provider choices so you can evaluate with the same model family
you serve with. Everything is lazy-imported so only the selected provider's
package is required.
"""
from __future__ import annotations
import logging

from .settings import Settings

logger = logging.getLogger(__name__)

_DEFAULT_MODELS = {
    "ollama": "llama3.1:8b",
    "openai": "gpt-4o-mini",
    "gemini": "gemini-1.5-flash",
    "anthropic": "claude-opus-4-8",
}


def build_judge_llm(cfg: Settings):
    provider = (cfg.judge_provider or "ollama").strip().lower()
    model = cfg.judge_model or _DEFAULT_MODELS.get(provider)
    logger.info("Judge LLM: provider=%s model=%s", provider, model)

    if provider == "ollama":
        from langchain_ollama import ChatOllama
        return ChatOllama(base_url=cfg.ollama_base_url, model=model, num_predict=cfg.judge_max_tokens)
    if provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model, api_key=cfg.openai_api_key, base_url=cfg.openai_base_url,
                          max_tokens=cfg.judge_max_tokens)
    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(model=model, google_api_key=cfg.google_api_key,
                                      max_output_tokens=cfg.judge_max_tokens)
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        # temperature omitted (Opus 4.8/4.7 reject it)
        return ChatAnthropic(model=model, api_key=cfg.anthropic_api_key, max_tokens=cfg.judge_max_tokens)
    raise ValueError(f"Unknown judge provider: {cfg.judge_provider}")


def build_judge_embeddings(cfg: Settings):
    provider = (cfg.embed_provider or "ollama").strip().lower()
    logger.info("Judge embeddings: provider=%s model=%s", provider, cfg.embed_model)
    if provider == "ollama":
        from langchain_ollama import OllamaEmbeddings
        return OllamaEmbeddings(base_url=cfg.ollama_base_url, model=cfg.embed_model)
    if provider == "openai":
        from langchain_openai import OpenAIEmbeddings
        return OpenAIEmbeddings(model=cfg.embed_model, api_key=cfg.openai_api_key,
                                base_url=cfg.openai_base_url)
    raise ValueError(f"Unknown embed provider: {cfg.embed_provider}")
