import argparse, logging, sys
from datetime import datetime
# from dotenv import load_dotenv

from .settings import settings
from .helpers import retry
from .clients.rabbitmq_client import init_rabbitmq
from .vector_indexer import DatabaseIndexer
from .consumer import consume_forever

# load_dotenv()

def init_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Consume embeddings and index them into a vector DB")
    parser.add_argument("--backend", help="Index backend (default: weaviate)")
    parser.add_argument("--collection", help="Override collection/index name (backend-specific)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB, only log")
    args = parser.parse_args()

    if args.backend:
        settings.backend = args.backend
    if args.collection:
        settings.collection = args.collection  # backend-specific override
    if args.dry_run:
        settings.dry_run = True

    logging.info("db-indexer started at %s", datetime.utcnow().isoformat())
    logging.info("Selected backend: %s", settings.backend)

    # Create and start indexer
    indexer = DatabaseIndexer(settings)
    indexer.start()

    # RabbitMQ consumer
    try:
        connection, channel = init_rabbitmq(settings)
    except Exception as e:
        logging.error("RabbitMQ init failed: %s", e)
        indexer.stop()
        raise

    try:
        consume_forever(channel, indexer, settings)
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down...")
    finally:
        try:
            channel.close()
        finally:
            connection.close()
        indexer.stop()
        sys.exit(0)
