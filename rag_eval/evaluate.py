"""Ragas evaluation of the live RAG pipeline.

Reads a dataset of questions (optionally with ground-truth answers), asks the
running `rag_api` for an answer + its retrieved contexts, then scores the results
with Ragas. Reference-based metrics are only run when ground truth is provided.

Dataset formats (auto-detected):
  * .jsonl : one JSON object per line
  * .json  : a list of objects
Each item: {"question": "...", "ground_truth": "..."}  (ground_truth optional;
"reference" is accepted as an alias).
"""
from __future__ import annotations
import json
import logging
import os
from typing import Any, Dict, List

import requests

from .judge import build_judge_embeddings, build_judge_llm
from .settings import Settings

logger = logging.getLogger(__name__)


def load_dataset(path: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        if path.endswith(".jsonl"):
            for line in fh:
                line = line.strip()
                if line:
                    items.append(json.loads(line))
        else:
            items = json.load(fh)
    if not items:
        raise ValueError(f"No samples found in dataset: {path}")
    logger.info("Loaded %d eval samples from %s", len(items), path)
    return items


def query_rag_api(cfg: Settings, question: str) -> Dict[str, Any]:
    payload = {"question": question, "return_contexts": True}
    for key in ("k", "alpha", "rerank", "top_n"):
        val = getattr(cfg, key)
        if val is not None:
            payload[key] = val
    resp = requests.post(
        f"{cfg.rag_api_url}/query", json=payload, timeout=cfg.request_timeout
    )
    resp.raise_for_status()
    return resp.json()


def build_rows(cfg: Settings, samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for i, s in enumerate(samples, start=1):
        question = s.get("question") or s.get("user_input")
        if not question:
            logger.warning("Skipping sample %d with no question", i)
            continue
        try:
            result = query_rag_api(cfg, question)
        except Exception as e:
            logger.error("Query failed for sample %d (%r): %s", i, question[:60], e)
            continue

        contexts = result.get("contexts")
        if not contexts:  # fall back to source snippets if full contexts absent
            contexts = [src.get("snippet", "") for src in result.get("sources", [])]

        row: Dict[str, Any] = {
            "user_input": question,
            "response": result.get("answer", ""),
            "retrieved_contexts": contexts or [""],
        }
        reference = s.get("ground_truth") or s.get("reference")
        if reference:
            row["reference"] = reference
        rows.append(row)
        logger.info("Collected %d/%d", i, len(samples))
    if not rows:
        raise RuntimeError("No rows collected — is rag_api reachable and populated?")
    return rows


def build_metrics(has_reference: bool):
    from ragas.metrics import (
        Faithfulness,
        LLMContextPrecisionWithoutReference,
        ResponseRelevancy,
    )

    # Reference-free metrics (work with question + answer + contexts).
    metrics = [Faithfulness(), ResponseRelevancy(), LLMContextPrecisionWithoutReference()]
    if has_reference:
        from ragas.metrics import LLMContextRecall
        metrics.append(LLMContextRecall())
    return metrics


def run_eval(cfg: Settings) -> Dict[str, float]:
    # Ragas runs metrics with asyncio; make it safe in any environment.
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except Exception:
        pass

    from ragas import EvaluationDataset, evaluate
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from ragas.llms import LangchainLLMWrapper

    samples = load_dataset(cfg.dataset_path)
    rows = build_rows(cfg, samples)
    has_reference = any("reference" in r for r in rows)
    logger.info("Evaluating %d rows (reference-based metrics: %s)", len(rows), has_reference)

    dataset = EvaluationDataset.from_list(rows)
    metrics = build_metrics(has_reference)
    llm = LangchainLLMWrapper(build_judge_llm(cfg))
    embeddings = LangchainEmbeddingsWrapper(build_judge_embeddings(cfg))

    result = evaluate(dataset=dataset, metrics=metrics, llm=llm, embeddings=embeddings)

    df = result.to_pandas()
    os.makedirs(os.path.dirname(cfg.output_path) or ".", exist_ok=True)
    df.to_csv(cfg.output_path, index=False)
    logger.info("Per-sample scores written to %s", cfg.output_path)

    # Aggregate means over numeric metric columns.
    import pandas as pd
    numeric = df.select_dtypes(include="number")
    aggregate = {col: round(float(numeric[col].mean()), 4) for col in numeric.columns}
    return aggregate
