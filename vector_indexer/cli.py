import argparse
import logging

from ragflow_contracts import obs

from .settings import settings
from .clients.rabbitmq_client import init_rabbitmq
from .vector_indexer import DatabaseIndexer
from .consumer import consume_forever


def main():
    obs.init_logging("vector_indexer", settings.log_level)
    parser = argparse.ArgumentParser(description="Consume embeddings and index them into a vector DB")
    parser.add_argument("--backend", help="Index backend (default: weaviate)")
    parser.add_argument("--collection", help="Override collection/index name")
    parser.add_argument("--batch-size", type=int, help="Override BATCH_SIZE")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB, only log")
    args = parser.parse_args()

    if args.backend:
        settings.backend = args.backend
    if args.collection:
        settings.collection = args.collection
    if args.batch_size:
        settings.batch_size = args.batch_size
    if args.dry_run:
        settings.dry_run = True

    logging.info("vector_indexer starting (backend=%s, batch_size=%d)", settings.backend, settings.batch_size)

    indexer = DatabaseIndexer(settings)
    indexer.start()

    obs.start_metrics_server(settings.metrics_port)

    try:
        prefetch = max(settings.batch_size, settings.rabbitmq_prefetch_count)
        connection, channel = init_rabbitmq(settings, prefetch=prefetch)
    except Exception as e:
        logging.error("RabbitMQ init failed: %s", e)
        indexer.stop()
        raise

    try:
        consume_forever(connection, channel, indexer, settings)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down...")
    finally:
        try:
            channel.close()
        finally:
            connection.close()
        indexer.stop()
