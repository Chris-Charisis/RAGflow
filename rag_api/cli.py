"""Entrypoint: configure logging and run the FastAPI app with uvicorn."""
from __future__ import annotations
import logging
import sys

import uvicorn

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
    logging.info("Starting RAGflow Query API on %s:%s", settings.api_host, settings.api_port)
    uvicorn.run(
        "rag_api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
