"""Query-side embeddings.

Deliberately re-implements the same Ollama `/api/embed` call the `embedder`
service uses at index time, so query vectors land in the *same* vector space as
the stored document vectors. The mxbai instruction prompt is applied on the
query side only (its intended use); make it configurable so it can be tuned or
disabled without code changes.
"""
from __future__ import annotations
import logging
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)


class EmbeddingError(RuntimeError):
    pass


class QueryEmbedder:
    def __init__(
        self,
        base_url: str,
        model: str,
        *,
        query_prompt: str = "",
        dimensions: Optional[int] = None,
        timeout_s: int = 60,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.query_prompt = query_prompt or ""
        self.dimensions = dimensions
        self.timeout_s = timeout_s

    def embed_query(self, text: str) -> List[float]:
        prompt = f"{self.query_prompt}{text}" if self.query_prompt else text
        payload = {"model": self.model, "input": [prompt], "truncate": True}
        if self.dimensions is not None:
            payload["dimensions"] = self.dimensions

        try:
            resp = requests.post(
                f"{self.base_url}/api/embed", json=payload, timeout=self.timeout_s
            )
        except requests.RequestException as e:
            raise EmbeddingError(f"HTTP error embedding query: {e}") from e

        if resp.status_code != 200:
            raise EmbeddingError(
                f"Ollama embed -> {resp.status_code}: {resp.text[:200]}"
            )

        embeddings = resp.json().get("embeddings")
        if not isinstance(embeddings, list) or not embeddings:
            raise EmbeddingError("Ollama response missing 'embeddings'")
        return embeddings[0]
