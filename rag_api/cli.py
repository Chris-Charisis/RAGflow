"""Entrypoint: configure logging and run the FastAPI app with uvicorn."""
from __future__ import annotations
import logging

import uvicorn
from ragflow_contracts import obs

from .settings import settings


def main() -> None:
    obs.init_logging("rag_api", settings.log_level)
    logging.info("Starting RAGflow Query API on %s:%s", settings.api_host, settings.api_port)
    uvicorn.run(
        "rag_api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
