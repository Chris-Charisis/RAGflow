"""Entrypoint for the Ragas evaluation harness."""
from __future__ import annotations
import argparse
import json
import logging
import sys

from .evaluate import run_eval
from .settings import settings


def init_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def main() -> None:
    init_logging()
    parser = argparse.ArgumentParser(description="Evaluate the RAG pipeline with Ragas")
    parser.add_argument("--dataset", help="Path to questions .jsonl/.json")
    parser.add_argument("--output", help="Path to write per-sample results CSV")
    parser.add_argument("--k", type=int, help="Override retrieval top-k")
    parser.add_argument("--alpha", type=float, help="Override hybrid alpha")
    parser.add_argument("--rerank", action="store_true", help="Force reranking on")
    args = parser.parse_args()

    if args.dataset:
        settings.dataset_path = args.dataset
    if args.output:
        settings.output_path = args.output
    if args.k is not None:
        settings.k = args.k
    if args.alpha is not None:
        settings.alpha = args.alpha
    if args.rerank:
        settings.rerank = True

    aggregate = run_eval(settings)
    print("\n===== Ragas aggregate scores =====")
    print(json.dumps(aggregate, indent=2))
