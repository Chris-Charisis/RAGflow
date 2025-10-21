import hashlib
import re
from typing import Any, Dict, List

class Chunker:
    """
    Stateful chunker. Configure once (strategy/size/overlap), then call `chunk_payload`.
    Exposes concrete strategy methods as instance methods so you can unit-test them directly.
    """

    def __init__(self, *, strategy: str = "sliding", size: int = 1200, overlap: int = 200):
        self.strategy = strategy.lower()
        self.size = int(size)
        self.overlap = int(overlap)
        if self.size <= 0:
            raise ValueError("size must be > 0")
        if self.strategy == "sliding" and not (0 <= self.overlap < self.size):
            raise ValueError("overlap must be >= 0 and < size")
        # Precompiled regex for sentence boundaries (simple heuristic)
        self._sent_pat = re.compile(r"[^.!?]+(?:[.!?]+|\Z)", re.S)

    # ---------- Public API ----------
    def chunk_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Takes an input message (schema/source/metadata/text) and returns a list of
        per-chunk messages with the same envelope plus doc_id and chunk metadata.
        """
        text = (payload.get("text") or "").strip()
        if not text:
            return []

        chunks = self.strategy_dispatch(text)
        did = self._doc_id(payload)

        base = {
            "schema": 1,
            "source": payload.get("source"),
            "metadata": payload.get("metadata"),
            "doc_id": did,
        }
        return [{**base, "chunk": ch} for ch in chunks]

    # ---------- Strategy dispatch ----------
    def strategy_dispatch(self, text: str) -> List[Dict[str, Any]]:
        if self.strategy == "sliding":
            return self.sliding(text, self.size, self.overlap)
        elif self.strategy == "sentence":
            return self.sentence(text, self.size)
        elif self.strategy == "recursive":
            return self.recursive(text, self.size)
        else:
            raise ValueError(f"Unknown chunk strategy: {self.strategy}")

    # ---------- Concrete strategies ----------
    def sliding_window(text: str, chunk_word_size: int = 288, chunk_overlap: int = 0) -> List[Dict[str, Any]]:
        words = text.split(" ")
        out: List[Dict[str, Any]] = []
        for idx, i in enumerate(range(0, len(words), chunk_word_size - chunk_overlap)):
            chunk = words[i:i + chunk_word_size]
            if chunk:
                out.append(
                    {
                        "index": idx,
                        "num_words": len(chunk),
                        "text": " ".join(chunk),
                    }
                )            
        return out

    def create_overlapping_chunks(text, MAX_CHUNK_SIZE=365, OVERLAP_MAX_SIZE=73):
        # List to hold the final output
        out: List[Dict[str, Any]] = []
        # Split the text using ". " and ".<Capital letter>" as delimiters
        pattern = r'(?<=\.)\s+|(?<=\.)(?=[A-Z])'
        sentences = [s for s in re.split(pattern, text) if s]
        # Initialize variables for chunking
        chunks = []
        chunk_words = 0
        for idx,sentence in enumerate(sentences):
            # Split the sentence into words
            words = sentence.split(" ")
            # Start the first chunk with the first sentence
            if chunk_words == 0:
                chunk_sentence_ids = [idx]
                chunk_words = len(words)
            # If the current chunk plus the new sentence is within the max size, add it
            elif chunk_words + len(words) < MAX_CHUNK_SIZE:
                chunk_sentence_ids.append(idx)
                chunk_words += len(words)
            # If adding the new sentence exceeds the max size, create a new chunk
            else:
                chunks.append(chunk_sentence_ids)
                chunk_sentence_ids = []
                overlapping_words = 0
                # Check how many sentences can be added from the end of the current chunk to the new chunk for overlapping
                for y in reversed(chunks[-1]):
                    overlapping_words += len(sentences[y].split(" "))
                    if overlapping_words < OVERLAP_MAX_SIZE:
                        chunk_sentence_ids.append(y)
                    else:
                        break
                chunk_sentence_ids.reverse()
                chunk_sentence_ids.append(idx)
                chunk_words = len(words)

        # Convert from sentence indices to actual text chunks
        text_chunks = []
        for idx, chunk in enumerate(chunks):
            text_chunks.append(" ".join([sentences[i] for i in chunk]))
            out.append(
                {
                    "index": idx,
                    "num_words": len(text_chunks[-1].split(" ")),
                    "text": text_chunks[-1],
                }
            )

        return out

    def recursive(self, text: str, size: int) -> List[Dict[str, Any]]:
        """
        Very simple hierarchical splitter that tries big separators first, then slices.
        Note: start/end offsets are approximate if separators are collapsed.
        Consider replacing with a more precise span-preserving splitter if needed.
        """
        seps = ["\n\n", "\n", ". "]
        pieces = [text]
        for sep in seps:
            next_pieces = []
            for p in pieces:
                if len(p) > size:
                    next_pieces.extend([q for q in p.split(sep) if q])
                else:
                    next_pieces.append(p)
            pieces = next_pieces

        out: List[Dict[str, Any]] = []
        idx = 0
        cursor = 0
        for p in pieces:
            s = 0
            while s < len(p):
                chunk_text = p[s : s + size]
                out.append(
                    {
                        "index": idx,
                        "start": cursor + s,
                        "end": cursor + s + len(chunk_text),
                        "num_chars": len(chunk_text),
                        "text": chunk_text,
                    }
                )
                idx += 1
                s += size
            # Advance cursor by the piece length; this ignores the exact separator length.
            cursor += len(p)
        return out

    # ---------- Helpers ----------

    @staticmethod
    def _doc_id(payload: Dict[str, Any]) -> str:
        src = payload.get("source", {})
        oid = src.get("object") or src.get("key")
        # remove .pdf from oid
        if oid and oid.endswith(".pdf"):
            oid = oid[:-4]
        if oid:
            bucket = src.get("bucket") or ""
            return f"{bucket}/{oid}"
        # fallback: stable hash of text
        txt = payload.get("text", "")
        return hashlib.sha1(txt.encode("utf-8")).hexdigest()[:16]
    
