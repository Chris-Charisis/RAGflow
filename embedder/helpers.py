import json
import logging

from ragflow_contracts import obs
from ragflow_contracts.mq import publish_json

from embedder.embedder.embedder import EmbeddingError
from .settings import settings

_processed = obs.counter("embedder_chunks_processed_total", "Chunks embedded and published")
_failed = obs.counter("embedder_batches_failed_total", "Embedding batches that failed")


def make_batch_processor(channel, embedder):
    """Build the batch handler used by mq.BatchConsumer.

    Embeds a batch of chunk messages in one model call and republishes each.
    Malformed messages are dropped (and acked); a transient embedding/publish
    failure raises so the whole batch is nacked and retried.
    """
    def process(bodies):
        payloads = []
        for body in bodies:
            try:
                payloads.append(json.loads(body))
            except Exception as e:
                logging.error("Invalid JSON on input; dropping message: %s", e)
        if not payloads:
            return
        try:
            out_msgs = embedder.process_batch(payloads)
        except EmbeddingError:
            _failed.inc()
            raise  # nack whole batch -> retry
        for msg in out_msgs:
            publish_json(
                channel,
                settings.rabbitmq_output_exchange,
                settings.rabbitmq_output_routing_key,
                msg,
            )
        _processed.inc(len(out_msgs))
        logging.info("Embedded & published %d chunk(s)", len(out_msgs))

    return process
