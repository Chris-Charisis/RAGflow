"""Shared observability: structured logging + optional Prometheus metrics.

Structured (JSON) logging has no external dependency. Prometheus metrics are
optional — if `prometheus_client` is not installed the helpers degrade to
no-ops, so this module is safe to import everywhere.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from typing import Optional

try:  # optional dependency
    from prometheus_client import Counter, Histogram, start_http_server  # type: ignore
    _PROM = True
except Exception:  # pragma: no cover
    _PROM = False


class _JsonFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def init_logging(service: str, level: str = "INFO", json_format: bool = True) -> None:
    """Configure root logging for a service (structured JSON by default)."""
    handler = logging.StreamHandler(sys.stdout)
    if json_format:
        handler.setFormatter(_JsonFormatter(service))
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s")
        )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )


# ---------- Metrics ----------

class _NoOpMetric:
    """Stand-in when prometheus_client is unavailable."""
    def labels(self, *a, **k): return self
    def inc(self, *a, **k): pass
    def observe(self, *a, **k): pass
    def time(self):
        class _C:
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False
        return _C()


def start_metrics_server(port: int) -> bool:
    """Expose Prometheus metrics on `port`. Returns True if started."""
    if not _PROM:
        logging.getLogger(__name__).info("prometheus_client not installed; metrics disabled")
        return False
    start_http_server(port)
    logging.getLogger(__name__).info("Prometheus metrics on :%d/metrics", port)
    return True


def counter(name: str, doc: str, labelnames: Optional[list[str]] = None):
    if not _PROM:
        return _NoOpMetric()
    return Counter(name, doc, labelnames or [])


def histogram(name: str, doc: str, labelnames: Optional[list[str]] = None):
    if not _PROM:
        return _NoOpMetric()
    return Histogram(name, doc, labelnames or [])
