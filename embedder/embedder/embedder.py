from typing import Any, Dict, List, Sequence, Optional
import logging
import time

from ..clients.ollama_client import OllamaClient, OllamaClientError


class EmbeddingError(RuntimeError):
    pass


class Embedder:
    """
    Core embedding logic, kept independent from RabbitMQ.
    """
    client: OllamaClient
    model: str
    dimensions: Optional[int] = None
    truncate: bool = True
    max_retries: int = 3
    backoff_s: float = 1.0

    # ---------- low-level embedding ----------

    def embed_texts(self, text: Sequence[str]) -> List[List[float]]:
        if not isinstance(text, str):
            raise EmbeddingError("Input text must be a string")

        ollama_payload: Dict[str, Any] = {
            "model": self.model,
            "input": [text],
            "truncate": self.truncate,
        }
        if self.dimensions is not None:
            ollama_payload["dimensions"] = self.dimensions

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                data = self.client.request("POST", "/api/embed", json=ollama_payload)
                embs = data.get("embeddings")
                if not isinstance(embs, list):
                    raise EmbeddingError("Ollama response missing 'embeddings'")
                return embs
            except (OllamaClientError, EmbeddingError) as e:
                last_err = e
                logging.warning("embed_texts attempt %d/%d failed: %s", attempt, self.max_retries, e)
                if attempt < self.max_retries:
                    time.sleep(self.backoff_s * attempt)

        raise EmbeddingError(f"Failed to embed after {self.max_retries} attempts: {last_err}")

    def embed_one(self, text: str) -> List[float]:
        vecs = self.embed_texts([text])
        return vecs[0] if vecs else []

    # ---------- message-level API (pure transform) ----------

    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            chunk = payload["chunk"]["texts"]
        except KeyError:
            raise EmbeddingError("Invalid payload structure")


        payload["metadata"]["embedding"]["embedding_model"] = self.model
        payload["metadata"]["embedding"]["embedding_vector"] = self.embed_texts(chunk)
        print(payload["metadata"]["embedding"]["embedding_vector"])
        print(len(payload["metadata"]["embedding"]["embedding_vector"]))
        payload["metadata"]["embedding"]["embedding_dim"] = len(payload["metadata"]["embedding"]["embedding_vector"][0]) if payload["metadata"]["embedding"]["embedding_vector"] and isinstance(payload["metadata"]["embedding"]["embedding_vector"][0], list) else 0

        return payload
