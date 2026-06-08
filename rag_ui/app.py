"""Minimal Streamlit chat UI for RAGflow.

Talks to the rag_api service. Streams the answer token-by-token via the
`/chat/stream` NDJSON endpoint and shows grounded sources underneath.
Retrieval/LLM knobs are exposed in the sidebar so they can be tuned per query
without redeploying.
"""
from __future__ import annotations
import json
import os

import requests
import streamlit as st

API_URL = os.environ.get("RAG_API_URL", "http://rag_api:8000")
REQUEST_TIMEOUT = int(os.environ.get("RAG_UI_TIMEOUT", "120"))

st.set_page_config(page_title="RAGflow", page_icon="🔎", layout="centered")
st.title("🔎 RAGflow")

with st.sidebar:
    st.header("Retrieval settings")
    alpha = st.slider(
        "Hybrid alpha (0=keyword, 1=vector)", min_value=0.0, max_value=1.0,
        value=0.5, step=0.05,
    )
    k = st.number_input("Candidates (top-k)", min_value=1, max_value=50, value=10)
    rerank = st.checkbox("Re-rank results", value=False)
    top_n = st.number_input(
        "Keep after re-rank (top-n)", min_value=1, max_value=20, value=4,
        disabled=not rerank,
    )
    st.caption(f"API: {API_URL}")
    try:
        health = requests.get(f"{API_URL}/health", timeout=5).json()
        st.success(f"LLM: {health.get('llm_provider')} · Weaviate: {health.get('weaviate_ready')}")
    except Exception as e:
        st.error(f"API unreachable: {e}")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Replay history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sources"):
            _render_sources = msg["sources"]
            with st.expander(f"Sources ({len(_render_sources)})"):
                for s in _render_sources:
                    st.markdown(
                        f"**[{s['n']}] {s.get('title') or s.get('object') or s.get('doc_id') or 'source'}**"
                        + (f" · score={s['score']:.3f}" if s.get('score') is not None else "")
                    )
                    if s.get("snippet"):
                        st.caption(s["snippet"])


def stream_answer(question: str):
    """Yield answer tokens; capture sources into the closure list."""
    payload = {
        "question": question,
        "k": int(k),
        "alpha": float(alpha),
        "rerank": bool(rerank),
        "top_n": int(top_n),
    }
    collected_sources = []
    with requests.post(
        f"{API_URL}/chat/stream", json=payload, stream=True, timeout=REQUEST_TIMEOUT
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            event = json.loads(line)
            if event["type"] == "token":
                yield event["data"]
            elif event["type"] == "sources":
                collected_sources.extend(event["data"])
    stream_answer.sources = collected_sources


prompt = st.chat_input("Ask a question about your documents…")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            answer = st.write_stream(stream_answer(prompt))
            sources = getattr(stream_answer, "sources", [])
        except Exception as e:
            answer = f"Request failed: {e}"
            sources = []
            st.error(answer)
        if sources:
            with st.expander(f"Sources ({len(sources)})"):
                for s in sources:
                    st.markdown(
                        f"**[{s['n']}] {s.get('title') or s.get('object') or s.get('doc_id') or 'source'}**"
                        + (f" · score={s['score']:.3f}" if s.get('score') is not None else "")
                    )
                    if s.get("snippet"):
                        st.caption(s["snippet"])

    st.session_state.messages.append(
        {"role": "assistant", "content": answer, "sources": sources}
    )
