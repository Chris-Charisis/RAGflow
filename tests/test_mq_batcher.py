"""Smoke tests for the shared micro-batching consumer (no real RabbitMQ)."""
import pytest

from ragflow_contracts.mq import BatchConsumer


class FakeChannel:
    def __init__(self):
        self.acked = []
        self.nacked = []
        self.consumed_queue = None
        self.cb = None

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=True):
        self.nacked.append((delivery_tag, requeue))

    def basic_consume(self, queue, on_message_callback, auto_ack):
        self.consumed_queue = queue
        self.cb = on_message_callback


class FakeConn:
    def call_later(self, delay, cb):  # no-op timer
        return None


class FakeMethod:
    def __init__(self, tag):
        self.delivery_tag = tag


def _feed(consumer, n):
    for i in range(1, n + 1):
        consumer._on_message(None, FakeMethod(i), None, b'{"i": %d}' % i)


def test_flushes_at_batch_size_and_acks_all():
    seen = []
    ch = FakeChannel()
    c = BatchConsumer(FakeConn(), ch, "q", lambda bodies: seen.append(len(bodies)), batch_size=3)
    _feed(c, 3)
    assert seen == [3]          # processed exactly one batch of 3
    assert ch.acked == [1, 2, 3]
    assert ch.nacked == []


def test_partial_batch_not_processed_until_flush():
    seen = []
    ch = FakeChannel()
    c = BatchConsumer(FakeConn(), ch, "q", lambda b: seen.append(len(b)), batch_size=5)
    _feed(c, 2)
    assert seen == []           # below batch_size -> buffered
    c.flush()                   # timer/shutdown flush
    assert seen == [2]
    assert ch.acked == [1, 2]


def test_process_error_nacks_whole_batch():
    def boom(bodies):
        raise RuntimeError("downstream down")
    ch = FakeChannel()
    c = BatchConsumer(FakeConn(), ch, "q", boom, batch_size=2, requeue_on_error=True)
    _feed(c, 2)
    assert ch.acked == []
    assert ch.nacked == [(1, True), (2, True)]


def test_batch_size_one_is_per_message():
    seen = []
    ch = FakeChannel()
    c = BatchConsumer(FakeConn(), ch, "q", lambda b: seen.append(len(b)), batch_size=1)
    _feed(c, 3)
    assert seen == [1, 1, 1]
    assert ch.acked == [1, 2, 3]
