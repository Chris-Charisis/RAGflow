"""Shared RabbitMQ helpers — de-duplicates the per-service connection,
topology-declaration, publish, and (new) batching boilerplate.

`cfg` is any object exposing the standard settings attributes
(`rabbitmq_host`, `rabbitmq_port`, `rabbitmq_vhost`, `rabbitmq_user`,
`rabbitmq_password`, `rabbitmq_prefetch_count`).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, List, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection

logger = logging.getLogger(__name__)


def connection_parameters(cfg) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=cfg.rabbitmq_host,
        port=cfg.rabbitmq_port,
        virtual_host=cfg.rabbitmq_vhost,
        credentials=pika.PlainCredentials(cfg.rabbitmq_user, cfg.rabbitmq_password),
        heartbeat=120,
        blocked_connection_timeout=300,
    )


def connect(cfg, *, prefetch: Optional[int] = None, confirm: bool = True):
    """Open a blocking connection + channel with QoS and publisher confirms."""
    connection = pika.BlockingConnection(connection_parameters(cfg))
    channel = connection.channel()
    if confirm:
        channel.confirm_delivery()
    channel.basic_qos(prefetch_count=prefetch or cfg.rabbitmq_prefetch_count)
    return connection, channel


def declare(channel: BlockingChannel, exchange: str, queue: str, routing_key: str,
            *, exchange_type: str = "topic") -> None:
    """Idempotently declare a durable exchange + queue + binding."""
    channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
    channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)


def publish_json(channel: BlockingChannel, exchange: str, routing_key: str,
                 msg: dict, message_id: Optional[str] = None) -> None:
    """Publish a JSON, persistent, mandatory message."""
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(msg),
        mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id,
        ),
    )


class BatchConsumer:
    """Micro-batching consumer over a BlockingChannel.

    Buffers up to `batch_size` messages (or until `flush_seconds` elapses) and
    hands the batch to `process` as a list of raw bodies. On success the whole
    batch is acked; on failure each message is nacked.

    batch_size=1 degrades to per-message processing. Works alongside other
    `basic_consume` callbacks on the same channel (e.g. a deletion handler),
    since it acks/nacks individual delivery tags (never `multiple=True`).
    """

    def __init__(
        self,
        connection: BlockingConnection,
        channel: BlockingChannel,
        queue: str,
        process: Callable[[List[bytes]], None],
        *,
        batch_size: int = 1,
        flush_seconds: float = 2.0,
        requeue_on_error: bool = True,
        on_flush: Optional[Callable[[int], None]] = None,
    ):
        self.connection = connection
        self.channel = channel
        self.queue = queue
        self.process = process
        self.batch_size = max(1, int(batch_size))
        self.flush_seconds = flush_seconds
        self.requeue_on_error = requeue_on_error
        self.on_flush = on_flush
        self._buf: List[tuple[int, bytes]] = []

    def _on_message(self, ch, method, props, body):
        self._buf.append((method.delivery_tag, body))
        if len(self._buf) >= self.batch_size:
            self.flush()

    def _on_timer(self):
        self.flush()
        self._schedule()

    def _schedule(self):
        self.connection.call_later(self.flush_seconds, self._on_timer)

    def flush(self):
        if not self._buf:
            return
        batch = self._buf
        self._buf = []
        tags = [t for t, _ in batch]
        bodies = [b for _, b in batch]
        try:
            self.process(bodies)
            for t in tags:
                self.channel.basic_ack(delivery_tag=t)
            if self.on_flush:
                self.on_flush(len(bodies))
        except Exception:
            logger.exception("Batch processing failed (size=%d); nacking", len(bodies))
            for t in tags:
                self.channel.basic_nack(delivery_tag=t, requeue=self.requeue_on_error)

    def register(self):
        """Attach the batch consumer to the channel (call before start_consuming)."""
        self.channel.basic_consume(queue=self.queue, on_message_callback=self._on_message, auto_ack=False)
        self._schedule()
