import argparse
import logging

from ragflow_contracts import obs
from ragflow_contracts.mq import BatchConsumer

from .settings import settings
from .clients.rabbitmq_client import init_rabbitmq
from .clients.ollama_client import OllamaClient
from .embedder import Embedder
from .helpers import make_batch_processor


def main() -> None:
    obs.init_logging("embedder", settings.log_level)
    parser = argparse.ArgumentParser(
        description="RAGflow Embedder: consume chunks, embed with Ollama (batched), republish."
    )
    parser.add_argument("--model", help="Override OLLAMA_MODEL")
    parser.add_argument("--dimensions", type=int, help="Vector dims if supported")
    parser.add_argument("--timeout", type=int, help="Ollama request timeout (seconds)")
    parser.add_argument("--batch-size", type=int, help="Override BATCH_SIZE")
    args = parser.parse_args()

    batch_size = args.batch_size or settings.batch_size

    logging.info("Initializing Ollama connection...")
    ollama = OllamaClient(
        base_url=settings.ollama_base_url,
        timeout_s=args.timeout or settings.ollama_timeout_seconds,
    )
    embedder = Embedder(
        client=ollama,
        model=args.model or settings.ollama_model,
        dimensions=args.dimensions or settings.ollama_dimensions,
        truncate=True,
    )

    obs.start_metrics_server(settings.metrics_port)

    logging.info("Initializing RabbitMQ client (batch_size=%d)...", batch_size)
    # Ensure prefetch >= batch_size so a full batch can be in flight.
    prefetch = max(batch_size, settings.rabbitmq_prefetch_count)
    connection, channel = init_rabbitmq(settings, prefetch=prefetch)

    consumer = BatchConsumer(
        connection,
        channel,
        settings.rabbitmq_input_queue,
        make_batch_processor(channel, embedder),
        batch_size=batch_size,
        flush_seconds=settings.batch_flush_seconds,
    )
    consumer.register()
    logging.info("Consuming from %s (batched)", settings.rabbitmq_input_queue)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Interrupted; shutting down...")
    except BaseException as e:
        logging.exception("Unexpected fatal error: %s", e)
    finally:
        try:
            channel.close()
        finally:
            connection.close()
