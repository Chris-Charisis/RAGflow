import logging
import re
from typing import Any, Dict, List, Optional

import tiktoken
from ragflow_contracts import Chunk, ChunkMessage, Metadata, Source, compute_doc_id

logger = logging.getLogger(__name__)


class Chunker:
    """
    Stateful chunker. Configure once (strategy/size/overlap), then call `chunk_payload`.

    v2 changes:
      * `tokens` strategy (default) is token-aware via tiktoken, with exact
        character offsets and per-chunk token counts.
      * Boilerplate (pre-introduction / post-references) stripping is OPTIONAL
        (`strip_boilerplate`) so the chunker works on non-academic documents too.
      * Output conforms to the shared `ChunkMessage` contract and carries
        eval/reproducibility markers (strategy, size, overlap).
    """

    def __init__(
        self,
        *,
        strategy: str = "tokens",
        size: int,
        overlap: int,
        strip_boilerplate: bool = True,
        encoding_name: str = "cl100k_base",
    ):
        self.strategy = strategy.lower()
        self.size = int(size)
        self.overlap = int(overlap)
        self.strip_boilerplate = strip_boilerplate
        if self.size <= 0:
            raise ValueError("size must be > 0")
        if not (0 <= self.overlap < self.size):
            raise ValueError("overlap must be >= 0 and < size")
        self._enc = tiktoken.get_encoding(encoding_name)

    # ---------- Public API ----------
    def chunk_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Take an ingest message and return per-chunk messages (contract dicts)."""
        text = (payload.get("text") or "").strip()
        if not text:
            return []

        source = Source(**(payload.get("source") or {}))
        metadata = Metadata(**(payload.get("metadata") or {}))
        doc_id = compute_doc_id(source.bucket, source.object)
        published_at = payload.get("published_at")

        raw_chunks = self.strategy_dispatch(text)

        messages: List[Dict[str, Any]] = []
        for ch in raw_chunks:
            msg = ChunkMessage(
                published_at=published_at,
                source=source,
                metadata=metadata,
                doc_id=doc_id,
                chunk=Chunk(**ch),
                chunk_strategy=self.strategy,
                chunk_size=self.size,
                chunk_overlap=self.overlap,
            )
            messages.append(msg.model_dump(by_alias=True, exclude_none=True))
        return messages

    # ---------- Pre-processing ----------
    def pre_process_text(self, text: str, strategy: str):
        """Split on the `===section===` markers emitted by pdf_reader.

        When `strip_boilerplate` is False we simply drop the markers and keep the
        whole body. When True we keep only introduction..references and (for the
        recursive strategy) preserve the chapter structure.
        """
        parts = text.split("===")

        if not self.strip_boilerplate:
            # Drop section-title markers, keep all content.
            body = " ".join(p for i, p in enumerate(parts))
            if strategy == "recursive":
                return {"full_text": re.sub(r"\s+", " ", body).strip()}
            return re.sub(r"\s+", " ", body).strip()

        # --- boilerplate stripping (academic-paper heuristic) ---
        try:
            introduction_index = next(i for i, s in enumerate(parts) if s == "introduction")
        except StopIteration:
            introduction_index = 0
        try:
            references_index = next(
                i for i, s in enumerate(parts)
                if s in ("references", "acknowledgments") and i > introduction_index
            )
        except StopIteration:
            references_index = len(parts)

        parts_with_chapters = parts[introduction_index:references_index]

        combined_parts_with_chapters: Dict[str, str] = {}
        if len(parts_with_chapters) % 2 == 0:
            for i in range(0, len(parts_with_chapters), 2):
                chapter_title = parts_with_chapters[i]
                chapter_text = parts_with_chapters[i + 1] if i + 1 < len(parts_with_chapters) else ""
                if chapter_title in combined_parts_with_chapters:
                    suffix = 2
                    new_title = f"{chapter_title}_{suffix}"
                    while new_title in combined_parts_with_chapters:
                        suffix += 1
                        new_title = f"{chapter_title}_{suffix}"
                    chapter_title = new_title
                combined_parts_with_chapters[chapter_title] = chapter_text
        else:
            combined_parts_with_chapters["full_text"] = " ".join(parts_with_chapters)

        if strategy == "recursive":
            return combined_parts_with_chapters

        parts_without_chapters = [p for p in parts_with_chapters if len(p) > 100]
        return "\n".join(parts_without_chapters)

    # ---------- Strategy selection ----------
    def strategy_dispatch(self, text: str) -> List[Dict[str, Any]]:
        processed = self.pre_process_text(text, self.strategy)
        if self.strategy == "tokens":
            return self.token_chunks(processed, self.size, self.overlap)
        if self.strategy == "words":
            return self._enrich(self.words_chunks(processed, self.size, self.overlap), processed)
        if self.strategy == "sentences":
            return self._enrich(self.sentence_chunks(processed, self.size, self.overlap), processed)
        if self.strategy == "recursive":
            return self.recursive_chunking(processed, self.size, self.overlap)
        raise ValueError(f"Unknown chunk strategy: {self.strategy}")

    # ---------- Token-aware chunking (default) ----------
    def token_chunks(self, text: str, size: int, overlap: int) -> List[Dict[str, Any]]:
        tokens = self._enc.encode(text)
        out: List[Dict[str, Any]] = []
        step = max(1, size - overlap)
        idx = 0
        for i in range(0, len(tokens), step):
            window = tokens[i:i + size]
            if not window:
                break
            start = len(self._enc.decode(tokens[:i]))
            chunk_text = self._enc.decode(window)
            out.append(
                {
                    "index": idx,
                    "text": chunk_text,
                    "start": start,
                    "end": start + len(chunk_text),
                    "num_chars": len(chunk_text),
                    "num_words": len(chunk_text.split()),
                    "num_tokens": len(window),
                }
            )
            idx += 1
            if i + size >= len(tokens):
                break
        return out

    # ---------- Legacy strategies ----------
    def words_chunks(self, text: str, chunk_word_size: int, chunk_overlap: int) -> List[Dict[str, Any]]:
        words = text.split(" ")
        out: List[Dict[str, Any]] = []
        for idx, i in enumerate(range(0, len(words), chunk_word_size - chunk_overlap)):
            chunk = words[i:i + chunk_word_size]
            if chunk:
                out.append({"index": idx, "text": " ".join(chunk)})
        return out

    def sentence_chunks(self, text: str, MAX_CHUNK_SIZE: int, OVERLAP_MAX_SIZE: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        pattern = r'(?<=\.)\s+|(?<=\.)(?=[A-Z])'
        sentences = [s for s in re.split(pattern, text) if s]
        if not sentences:
            return out

        chunks: List[List[int]] = []
        chunk_words = 0
        chunk_sentence_ids: List[int] = []
        for idx, sentence in enumerate(sentences):
            words = sentence.split(" ")
            if chunk_words == 0:
                chunk_sentence_ids = [idx]
                chunk_words = len(words)
            elif chunk_words + len(words) < MAX_CHUNK_SIZE:
                chunk_sentence_ids.append(idx)
                chunk_words += len(words)
            else:
                chunks.append(chunk_sentence_ids)
                chunk_sentence_ids = []
                overlapping_words = 0
                for y in reversed(chunks[-1]):
                    if overlapping_words + len(sentences[y].split(" ")) < OVERLAP_MAX_SIZE:
                        overlapping_words += len(sentences[y].split(" "))
                        chunk_sentence_ids.append(y)
                    else:
                        break
                chunk_sentence_ids.reverse()
                chunk_sentence_ids.append(idx)
                chunk_words = len(words) + overlapping_words
        chunks.append(chunk_sentence_ids)

        for idx, chunk in enumerate(chunks):
            text_chunk = " ".join(sentences[i] for i in chunk)
            out.append({"index": idx, "text": text_chunk})
        return out

    def recursive_chunking(self, text: dict, MAX_CHUNK_SIZE=350, OVERLAP_MAX_SIZE=0) -> List[Dict[str, Any]]:
        all_chunks_list: List[Dict[str, Any]] = []
        global_idx = 0
        for chapter_title, chapter_text in text.items():
            chunks = self._enrich(
                self.sentence_chunks(chapter_text, MAX_CHUNK_SIZE, OVERLAP_MAX_SIZE),
                chapter_text,
            )
            for chunk in chunks:
                chunk["chapter"] = chapter_title
                chunk["index"] = global_idx
                global_idx += 1
                all_chunks_list.append(chunk)
        return all_chunks_list

    # ---------- Enrichment (offsets / counts for legacy strategies) ----------
    def _enrich(self, chunks: List[Dict[str, Any]], source_text: str) -> List[Dict[str, Any]]:
        cursor = 0
        for ch in chunks:
            txt = ch["text"]
            start = source_text.find(txt, cursor)
            if start == -1:
                start = source_text.find(txt)
            if start != -1:
                ch["start"] = start
                ch["end"] = start + len(txt)
                cursor = ch["end"]
            ch["num_chars"] = len(txt)
            ch["num_words"] = len(txt.split())
            ch["num_tokens"] = len(self._enc.encode(txt))
        return chunks
