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
    def __init__(self, client: OllamaClient, model: str, dimensions: Optional[int] = None, truncate: bool = True, max_retries: int = 3, backoff_s: float = 1.0):
        self.client = client
        self.model = model
        self.dimensions = dimensions
        self.truncate = truncate
        self.max_retries = max_retries
        self.backoff_s = backoff_s


    # ---------- low-level embedding ----------

    def embed_texts(self, text: Sequence[str]) -> List[List[float]]:
        if not isinstance(text, str):
            raise EmbeddingError("Input text must be a string but got: ", type(text))

        # Documents are embedded WITHOUT the mxbai retrieval instruction — that
        # prompt is for the QUERY side only (applied in rag_api). Prepending it to
        # documents creates a query/document asymmetry that degrades recall.
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

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed many texts in a single Ollama call (one vector per input)."""
        if not texts:
            return []
        ollama_payload: Dict[str, Any] = {
            "model": self.model,
            "input": list(texts),
            "truncate": self.truncate,
        }
        if self.dimensions is not None:
            ollama_payload["dimensions"] = self.dimensions

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                data = self.client.request("POST", "/api/embed", json=ollama_payload)
                embs = data.get("embeddings")
                if not isinstance(embs, list) or len(embs) != len(texts):
                    raise EmbeddingError(
                        f"Ollama returned {len(embs) if isinstance(embs, list) else 'none'} "
                        f"embeddings for {len(texts)} inputs"
                    )
                return embs
            except (OllamaClientError, EmbeddingError) as e:
                last_err = e
                logging.warning("embed_batch attempt %d/%d failed: %s", attempt, self.max_retries, e)
                if attempt < self.max_retries:
                    time.sleep(self.backoff_s * attempt)
        raise EmbeddingError(f"Failed to embed batch after {self.max_retries} attempts: {last_err}")

    # ---------- message-level API (pure transform) ----------

    def process_message(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.process_batch([payload])[0]

    def process_batch(self, payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Embed a batch of chunk payloads in one model call; attach embeddings."""
        texts: List[str] = []
        for p in payloads:
            try:
                texts.append(p["chunk"]["text"])
            except (KeyError, TypeError):
                raise EmbeddingError("Invalid payload structure (missing chunk.text)")

        vectors = self.embed_batch(texts)
        for p, vec in zip(payloads, vectors):
            p["embedding"] = {
                "embedding_model": self.model,
                # Keep the list-of-vectors shape the indexer expects (extract_vector reads [0]).
                "embedding_vector": [vec],
                "embedding_dim": len(vec) if isinstance(vec, list) else 0,
            }
        return payloads
