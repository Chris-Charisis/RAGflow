"""Provider-agnostic chat-model factory.

Returns a LangChain `BaseChatModel` for the configured provider. Each provider's
integration package is imported lazily, so only the selected provider's package
needs to be installed/configured at runtime.

  - ollama    : ChatOllama   (default; talks to the in-stack Ollama)
  - openai    : ChatOpenAI
  - gemini    : ChatGoogleGenerativeAI
  - anthropic : ChatAnthropic (default model: claude-opus-4-8)

Add a provider by extending `build_llm`.
"""
from __future__ import annotations
import logging

from langchain_core.language_models.chat_models import BaseChatModel

from ..settings import Settings

logger = logging.getLogger(__name__)

# Sensible per-provider defaults when LLM_MODEL is not set.
_DEFAULT_MODELS = {
    "ollama": "llama3.1:8b",
    "openai": "gpt-4o-mini",
    "gemini": "gemini-2.5-flash",
    "anthropic": "claude-opus-4-8",
}


def build_llm(cfg: Settings) -> BaseChatModel:
    provider = (cfg.llm_provider or "ollama").strip().lower()
    model = cfg.llm_model or _DEFAULT_MODELS.get(provider)
    logger.info("Using LLM provider=%s model=%s", provider, model)

    if provider == "ollama":
        from langchain_ollama import ChatOllama

        return ChatOllama(
            base_url=cfg.ollama_chat_base_url,
            model=model,
            temperature=cfg.llm_temperature,
            num_predict=cfg.llm_max_tokens,
        )

    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=model,
            api_key=cfg.openai_api_key,
            base_url=cfg.openai_base_url,
            temperature=cfg.llm_temperature,
            max_tokens=cfg.llm_max_tokens,
        )

    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(
            model=model,
            google_api_key=cfg.google_api_key,
            temperature=cfg.llm_temperature,
            max_output_tokens=cfg.llm_max_tokens,
        )

    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        # NOTE: Opus 4.8/4.7 reject `temperature` (returns 400), so it is not
        # passed here. Steer behavior via the prompt instead.
        return ChatAnthropic(
            model=model,
            api_key=cfg.anthropic_api_key,
            max_tokens=cfg.llm_max_tokens,
        )

    raise ValueError(f"Unknown LLM provider: {cfg.llm_provider}")