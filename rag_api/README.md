# rag_api — RAGflow Query/Answer Service

The retrieval + generation half of RAGflow (the "R" and "G"). It serves
questions against the Weaviate collection the ingestion pipeline fills.

## Flow

```
question
  -> embed query (same Ollama model as the embedder, query-side prompt)
  -> Weaviate hybrid search (BM25 + vector, self-provided query vector)
  -> [optional] rerank (flashrank | cross_encoder | cohere)
  -> prompt assembly with numbered context
  -> LLM (ollama | openai | gemini | anthropic)
  -> {answer, sources}
```

## Endpoints

| Method | Path          | Description                                      |
| ------ | ------------- | ------------------------------------------------ |
| GET    | `/health`     | Liveness + Weaviate readiness                    |
| POST   | `/query`      | Non-streaming `{answer, sources}`                |
| POST   | `/chat/stream`| NDJSON stream of `{type: token\|sources, data}`  |

Request body (all but `question` optional, overriding env defaults):

```json
{ "question": "…", "k": 10, "alpha": 0.5, "rerank": true, "top_n": 4 }
```

`alpha`: 1.0 = pure vector, 0.0 = pure keyword, 0.5 = balanced hybrid.

## Key configuration (env)

| Var | Default | Notes |
| --- | --- | --- |
| `COLLECTION` | `recursive_with_overlap` | Must match `vector_indexer` |
| `EMBED_MODEL` | `mxbai-embed-large:335m` | Must match `embedder` |
| `RETRIEVAL_ALPHA` / `RETRIEVAL_TOP_K` | `0.5` / `10` | Hybrid search |
| `RERANK_ENABLED` | `false` | Turn on reranking |
| `RERANK_PROVIDER` | `flashrank` | `flashrank` \| `cross_encoder` \| `cohere` |
| `LLM_PROVIDER` | `ollama` | `ollama` \| `openai` \| `gemini` \| `anthropic` |
| `LLM_MODEL` | per-provider default | e.g. `claude-opus-4-8` for anthropic |

Provider API keys: `OPENAI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`,
`COHERE_API_KEY` — only the selected provider's key is needed.
