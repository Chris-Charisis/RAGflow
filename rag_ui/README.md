# rag_ui — RAGflow Streamlit UI

A minimal custom chat front-end for RAGflow, replacing OpenWebUI. It calls the
`rag_api` service (so it goes through the full retrieval+generation pipeline,
not the bare LLM) and streams answers with their grounded sources.

Sidebar exposes per-query knobs: hybrid `alpha`, candidate `top-k`, and an
optional re-rank toggle with `top-n`.

## Config (env)

| Var | Default | Notes |
| --- | --- | --- |
| `RAG_API_URL` | `http://rag_api:8000` | rag_api base URL |
| `RAG_UI_TIMEOUT` | `120` | Per-request timeout (s) |

Runs on port `8501`.
