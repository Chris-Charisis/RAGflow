from __future__ import annotations
import logging

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties

from ragflow_contracts import DeletionMessage, compute_doc_id, obs
from ragflow_contracts.mq import BatchConsumer

from .helpers import load_json_bytes
from .vector_indexer import DatabaseIndexer
from .settings import Settings

logger = logging.getLogger(__name__)

_indexed = obs.counter("indexer_chunks_indexed_total", "Chunks upserted into the vector DB")
_deletes = obs.counter("indexer_deletions_total", "Deletion events processed")
_del_failed = obs.counter("indexer_deletions_failed_total", "Deletion events that failed")


def make_batch_processor(indexer: DatabaseIndexer):
    def process(bodies: list[bytes]):
        payloads = []
        for body in bodies:
            try:
                payloads.append(load_json_bytes(body))
            except Exception as e:
                logger.error("Invalid JSON on embeddings input; dropping: %s", e)
        if not payloads:
            return
        n = indexer.upsert_many(payloads)  # raises on failure -> batch nacked & retried
        _indexed.inc(n)
        logger.debug("Indexed %d chunk(s)", n)
    return process


def make_deletion_handler(indexer: DatabaseIndexer):
    def handle_deletion(ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            msg = DeletionMessage.model_validate(load_json_bytes(body))
            doc_id = msg.doc_id or compute_doc_id(msg.source.bucket, msg.source.object)
            if not doc_id:
                logger.warning("Deletion event without resolvable doc_id; acking")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            removed = indexer.delete(doc_id, msg.deleted_at)
            _deletes.inc()
            logger.info("Deletion processed doc_id=%s removed=%s", doc_id, removed)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            _del_failed.inc()
            logger.exception("Failed to process deletion: %s", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    return handle_deletion


def consume_forever(connection: BlockingConnection, channel: BlockingChannel,
                    indexer: DatabaseIndexer, cfg: Settings):
    # Embeddings: micro-batched upserts.
    batch = BatchConsumer(
        connection,
        channel,
        cfg.rabbitmq_input_queue,
        make_batch_processor(indexer),
        batch_size=cfg.batch_size,
        flush_seconds=cfg.batch_flush_seconds,
    )
    batch.register()
    # Deletions: handled individually (rare, must apply promptly).
    channel.basic_consume(
        queue=cfg.rabbitmq_delete_queue,
        on_message_callback=make_deletion_handler(indexer),
        auto_ack=False,
    )
    logger.info(
        "Consuming embeddings from '%s' (batch_size=%d) and deletions from '%s'",
        cfg.rabbitmq_input_queue, cfg.batch_size, cfg.rabbitmq_delete_queue,
    )
    channel.start_consuming()
