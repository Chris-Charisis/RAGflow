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

        chunks = self._dispatch(text)
        did = self._doc_id(payload)

        base = {
            "schema": 1,
            "source": payload.get("source"),
            "metadata": payload.get("metadata"),
            "doc_id": did,
        }
        return [{**base, "chunk": ch} for ch in chunks]

    # ---------- Strategy dispatch ----------
    def _dispatch(self, text: str) -> List[Dict[str, Any]]:
        if self.strategy == "sliding":
            return self.sliding(text, self.size, self.overlap)
        elif self.strategy == "sentence":
            return self.sentence(text, self.size)
        elif self.strategy == "recursive":
            return self.recursive(text, self.size)
        else:
            raise ValueError(f"Unknown chunk strategy: {self.strategy}")

    # ---------- Concrete strategies ----------
    def sliding(self, text: str, size: int, overlap: int) -> List[Dict[str, Any]]:
        """Character-based sliding window with overlap; exact start/end offsets."""
        step = size - overlap
        out: List[Dict[str, Any]] = []
        n = len(text)
        i = 0
        idx = 0
        while i < n:
            start = i
            end = min(i + size, n)
            chunk_text = text[start:end]
            out.append(
                {
                    "index": idx,
                    "start": start,
                    "end": end,
                    "num_chars": len(chunk_text),
                    "text": chunk_text,
                }
            )
            idx += 1
            i += step
        return out

    def sentence(self, text: str, size: int) -> List[Dict[str, Any]]:
        """
        Sentence-based chunking using regex spans; keeps true start/end offsets
        by grouping adjacent sentence spans up to `size` characters.
        """
        out: List[Dict[str, Any]] = []
        idx = 0
        group_start = None
        group_end = None
        group_len = 0

        for m in self._sent_pat.finditer(text):
            s, e = m.span()
            seg_len = e - s
            if group_start is None:
                group_start, group_end, group_len = s, e, seg_len
                continue

            if group_len + seg_len > size and group_len > 0:
                # flush current group
                chunk_text = text[group_start:group_end]
                out.append(
                    {
                        "index": idx,
                        "start": group_start,
                        "end": group_end,
                        "num_chars": len(chunk_text),
                        "text": chunk_text,
                    }
                )
                idx += 1
                # start new group with current sentence
                group_start, group_end, group_len = s, e, seg_len
            else:
                group_end = e
                group_len += seg_len

        # flush tail
        if group_start is not None:
            chunk_text = text[group_start:group_end]
            out.append(
                {
                    "index": idx,
                    "start": group_start,
                    "end": group_end,
                    "num_chars": len(chunk_text),
                    "text": chunk_text,
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
    
