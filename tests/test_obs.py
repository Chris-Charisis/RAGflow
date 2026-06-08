"""Smoke tests for observability helpers (degrade gracefully without prometheus)."""
from ragflow_contracts import obs


def test_init_logging_runs():
    obs.init_logging("test-svc", level="INFO", json_format=True)


def test_counter_and_histogram_usable():
    c = obs.counter("ragflow_test_counter_total", "test")
    c.inc()
    c.inc(3)
    h = obs.histogram("ragflow_test_hist_seconds", "test")
    with h.time():
        pass  # context-manager works in both real and no-op modes
    h.observe(0.01)


def test_labeled_counter_usable():
    c = obs.counter("ragflow_test_labeled_total", "test", ["endpoint"])
    # Real prometheus requires .labels(); the no-op accepts it too.
    if obs._PROM:
        c.labels("query").inc()
    else:
        c.labels("query").inc()
