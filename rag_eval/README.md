# rag_eval — Ragas evaluation harness

Scores the **live** RAG pipeline end-to-end: it sends each question to `rag_api`,
captures the generated answer and the contexts that were actually retrieved
(`/query?return_contexts=true`), and runs [Ragas](https://docs.ragas.io) metrics.

## Metrics

Always (reference-free):
- **faithfulness** — is the answer grounded in the retrieved contexts?
- **answer relevancy** (`ResponseRelevancy`) — does the answer address the question?
- **LLM context precision (without reference)** — are the retrieved contexts relevant?

Added when a sample has `ground_truth` / `reference`:
- **LLM context recall** — did retrieval surface the information needed for the reference answer?

These pair with the per-chunk eval markers the indexer now stores
(`doc_id`, `chunk_index`, offsets, `chunk_strategy/size/overlap`, `embedding_model`)
so retrieval changes can be compared run-to-run.

## Dataset

`.jsonl` (one object per line) or `.json` (a list). Each item:

```json
{"question": "What dataset was used?", "ground_truth": "optional reference answer"}
```

A starter file lives at `rag_eval/sample_questions.jsonl`.

## Run it

The service is gated behind the `eval` compose profile so it never starts with
the stack. Put your dataset in `./eval_data/questions.jsonl`, then:

```bash
docker compose run --rm rag_eval --dataset /data/questions.jsonl --output /data/results.csv
```

Aggregate scores print to stdout; per-sample scores are written to the CSV.

## Judge model

The judge LLM and embeddings are provider-agnostic (`EVAL_LLM_PROVIDER`,
`EVAL_EMBED_PROVIDER` — `ollama` default, or `openai`/`gemini`/`anthropic`).
Use a strong judge (e.g. `anthropic` + `claude-opus-4-8`) for trustworthy scores;
the local default (`ollama`/`llama3.1:8b`) works but is a weaker grader.
