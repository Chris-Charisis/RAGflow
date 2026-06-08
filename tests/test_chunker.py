"""Smoke tests for token-aware chunking + boilerplate stripping."""
from text_chunker.chunker.chunker import Chunker


def test_token_chunks_have_offsets_and_counts():
    c = Chunker(strategy="tokens", size=20, overlap=5, strip_boilerplate=False)
    payload = {
        "text": "alpha beta gamma " * 60,
        "source": {"bucket": "b", "object": "o.pdf"},
        "published_at": 7,
        "metadata": {"title": "T"},
    }
    msgs = c.chunk_payload(payload)
    assert len(msgs) > 1
    first = msgs[0]
    assert first["doc_id"] == "b/o"
    assert first["published_at"] == 7
    ch = first["chunk"]
    assert ch["num_tokens"] == 20
    assert ch["start"] == 0 and ch["end"] == ch["start"] + len(ch["text"])
    # indices are sequential
    assert [m["chunk"]["index"] for m in msgs[:3]] == [0, 1, 2]


def test_boilerplate_stripping_drops_references():
    c = Chunker(strategy="tokens", size=40, overlap=5, strip_boilerplate=True)
    text = "junk header ===introduction=== " + ("body " * 80) + " ===references=== refs only here"
    msgs = c.chunk_payload({"text": text, "source": {"bucket": "b", "object": "x.pdf"}, "published_at": 1})
    joined = " ".join(m["chunk"]["text"].lower() for m in msgs)
    assert "refs only here" not in joined  # references section removed
    assert "body" in joined


def test_empty_text_yields_no_chunks():
    c = Chunker(strategy="tokens", size=10, overlap=2)
    assert c.chunk_payload({"text": "   ", "source": {}}) == []
