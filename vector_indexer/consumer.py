from __future__ import annotations
import logging
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from .helpers import load_json_bytes
from .vector_indexer import DatabaseIndexer
from .settings import Settings

logger = logging.getLogger(__name__)

def handle_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes,
                   indexer: DatabaseIndexer, cfg: Settings):
    try:
        payload = load_json_bytes(body)
        indexer.upsert(payload)
        # Acknowledge message upon successful processing
        logger.debug("Message processed and indexed successfully.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Failed to process message: %s", e)
        # Requeue so you can inspect/retry; adjust to DLQ if you use one.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # standard nack semantics. :contentReference[oaicite:2]{index=2}

def consume_forever(channel: BlockingChannel, indexer: DatabaseIndexer, cfg: Settings):
    channel.basic_consume(
        queue=cfg.rabbitmq_input_queue,
        on_message_callback=lambda ch, m, p, b: handle_message(ch, m, p, b, indexer, cfg),
        auto_ack=False,
    )
    logger.info("Consuming from %s with routing key %s", cfg.rabbitmq_input_queue, cfg.rabbitmq_input_routing_key)
    channel.start_consuming()
