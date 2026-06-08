from __future__ import annotations
import logging
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from ragflow_contracts import DeletionMessage, compute_doc_id

from .helpers import load_json_bytes
from .vector_indexer import DatabaseIndexer
from .settings import Settings

logger = logging.getLogger(__name__)


def handle_message(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes,
                   indexer: DatabaseIndexer, cfg: Settings):
    """Index one embedded chunk (idempotent upsert; tombstone-aware)."""
    try:
        payload = load_json_bytes(body)
        indexer.upsert(payload)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Failed to index message: %s", e)
        # Requeue for retry/inspection; swap to a DLQ once one is configured.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def handle_deletion(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes,
                    indexer: DatabaseIndexer, cfg: Settings):
    """Process a deletion event: tombstone the doc and remove its chunks.

    The tombstone (with deleted_at) ensures chunks of the deleted generation
    that are still in flight get rejected at index time — covering the case
    where the PDF is deleted while the pipeline is still processing it.
    """
    try:
        msg = DeletionMessage.model_validate(load_json_bytes(body))
        doc_id = msg.doc_id or compute_doc_id(msg.source.bucket, msg.source.object)
        if not doc_id:
            logger.warning("Deletion event without resolvable doc_id; acking and skipping")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        removed = indexer.delete(doc_id, msg.deleted_at)
        logger.info("Deletion processed for doc_id=%s (removed=%s, deleted_at=%s)",
                    doc_id, removed, msg.deleted_at)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Failed to process deletion: %s", e)
        # Don't infinitely requeue a malformed deletion event.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def consume_forever(channel: BlockingChannel, indexer: DatabaseIndexer, cfg: Settings):
    channel.basic_consume(
        queue=cfg.rabbitmq_input_queue,
        on_message_callback=lambda ch, m, p, b: handle_message(ch, m, p, b, indexer, cfg),
        auto_ack=False,
    )
    channel.basic_consume(
        queue=cfg.rabbitmq_delete_queue,
        on_message_callback=lambda ch, m, p, b: handle_deletion(ch, m, p, b, indexer, cfg),
        auto_ack=False,
    )
    logger.info(
        "Consuming embeddings from '%s' and deletions from '%s'",
        cfg.rabbitmq_input_queue, cfg.rabbitmq_delete_queue,
    )
    channel.start_consuming()
