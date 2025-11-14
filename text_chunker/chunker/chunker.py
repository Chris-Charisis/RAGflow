import hashlib
import re
from typing import Any, Dict, List

class Chunker:
    """
    Stateful chunker. Configure once (strategy/size/overlap), then call `chunk_payload`.
    Exposes concrete strategy methods as instance methods so you can unit-test them directly.
    """

    def __init__(self, *, strategy: str = "words", size: int = 1200, overlap: int = 200):
        self.strategy = strategy.lower()
        self.size = int(size)
        self.overlap = int(overlap)
        if self.size <= 0:
            raise ValueError("size must be > 0")
        if not (0 <= self.overlap < self.size):
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

    # ---------- Pre-processing text for chunking strategies ----------
    def pre_process_text(self, text: str, strategy: str) -> str:
        parts = text.split("===")
        # Find the index of "introduction" and "references"/"acknowledgments" sections
        try:
            introduction_index = next(i for i, section in enumerate(parts) if section == 'introduction')
        except:
            introduction_index = 0
        try:
            references_index = next(i for i, section in enumerate(parts) if section == 'references' or section == 'acknowledgments')
        except:
            references_index = len(parts)

        # Remove sections before "introduction" and after "references" or "acknowledgments"
        parts_with_chapters = parts[introduction_index:references_index]
        # If chapters were found, combine into dictionary every two elements of the list "chapter":"relevant text"
        combined_parts_with_chapters = {}
        if len(parts_with_chapters) >= 2:
            for i in range(0, len(parts_with_chapters), 2):
                chapter_title = parts_with_chapters[i]
                chapter_text = parts_with_chapters[i+1] if i+1 < len(parts_with_chapters) else ""
                combined_parts_with_chapters[chapter_title] = chapter_text
        else:
            combined_parts_with_chapters["full_text"] = parts_with_chapters[0]

        # Keep the chapters information for recursive strategy
        if strategy == "recursive":
            return combined_parts_with_chapters

        # Return the whole combined text by combining the remaining sections with text above 100 characters
        # For strategies other than "recursive", do not take into consideration chapter hierarchy
        else:
            parts_without_chapters = [part for part in parts_with_chapters if len(part) > 100]
            combined_parts_without_chapters = "\n".join(parts_without_chapters)
            return combined_parts_without_chapters

    # ---------- Strategy selection ----------
    def strategy_dispatch(self, text: str) -> List[Dict[str, Any]]:
        text = self.pre_process_text(text, self.strategy)
        if self.strategy == "words":
            return self.words_chunks(text, self.size, self.overlap)
        elif self.strategy == "sentences":
            return self.sentence_chunks(text, self.size, self.overlap)
        elif self.strategy == "recursive":
            return self.recursive_chunking(text, self.size, self.overlap)
        else:
            raise ValueError(f"Unknown chunk strategy: {self.strategy}")

    # ---------- Chunking strategies strategies ----------
    # Create chunks based on words with/out overlap
    def words_chunks(self, text: str, chunk_word_size: int = 288, chunk_overlap: int = 0) -> List[Dict[str, Any]]:
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

    # Create chunks based on sentences with/out overlap
    def sentence_chunks(self,text, MAX_CHUNK_SIZE=350, OVERLAP_MAX_SIZE=0) -> List[Dict[str, Any]]:
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

        # Add the last chunk 
        chunks.append(chunk_sentence_ids)

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

    # Create sentence aware recursive chunking function for each element of the dictionary
    def recursive_chunking(self, text: dict, MAX_CHUNK_SIZE=350, OVERLAP_MAX_SIZE=0) -> List[Dict[str, Any]]:
        all_chunks_list = []
        # if isinstance(text, dict):
        all_chunks = {}
        for chapter_title, chapter_text in text.items():
            chunks = self.sentence_chunks(chapter_text, MAX_CHUNK_SIZE=MAX_CHUNK_SIZE, OVERLAP_MAX_SIZE=OVERLAP_MAX_SIZE)
            all_chunks[chapter_title] = chunks

        # add dictionary key as field in each chunk and create a flat list of all chunks
        for chapter_title, chunks in all_chunks.items():
            for chunk in chunks:
                all_chunks_list.append({**chunk, "chapter": chapter_title})                
        # else:
        #     # If text is not a dictionary, process it as a single chunk
        #     chunks = self.sentence_chunks(text, MAX_CHUNK_SIZE=MAX_CHUNK_SIZE, OVERLAP_MAX_SIZE=OVERLAP_MAX_SIZE)
        #     for chunk in chunks:
        #         all_chunks_list.append({**chunk, "chapter": "full_text"})   


        return all_chunks_list

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
    
