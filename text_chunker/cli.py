import argparse, json, logging, sys
from datetime import datetime
from functools import partial
# from dotenv import load_dotenv
# load_dotenv()
from .settings import settings
from .chunker import Chunker
from .clients.rabbitmq_client import init_rabbitmq
from .helpers import (
    process_message,
)

# Configure a JSON‑friendly stream handler using the log level retrieved from Settings.
# Kept in its own function so tests can call it without executing the rest of the CLI logic.
def init_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def main()-> None:
    init_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", choices=["words", "sentences", "recursive"], help="Chunking strategy")
    parser.add_argument("--size", type=int, help="Chunk size in words")
    parser.add_argument("--overlap", type=int, help="Overlap in words (if sentence strategy, actual_overlap <= overlap)")
    parser.add_argument("--prefetch", type=int, help="Consumer prefetch")
    args = parser.parse_args()

    logging.info("Initializing Chunker...")
    # Build one Chunker instance for the whole process
    chunker = Chunker(
        strategy=args.strategy or settings.chunk_strategy,
        size=args.size or settings.chunk_size,
        overlap=args.overlap or settings.chunk_overlap,
    )

    logging.info("Initializing RabbitMQ client...")
    try:
        # Establish RabbitMQ connection
        connection, channel = init_rabbitmq(settings)
    except Exception as e:
        logging.error("RabbitMQ initialization failed: %s", e)
        raise

    try:
        # Set how many messages are prefetched before they start being consumed
        channel.basic_qos(prefetch_count=args.prefetch or settings.prefetch_count)
        logging.info("Chunker started at %s", datetime.utcnow().isoformat())

        # Expand callback with partial to include arguments variable and be compatible with basic_consume
        cb = partial(process_message, chunker=chunker)
        channel.basic_consume(
            queue=settings.input_queue,
            on_message_callback=cb,
            auto_ack=False,
        )
        logging.info("Consuming from %s with routing key %s",
                     settings.input_queue, settings.input_routing_key)
        channel.start_consuming()
    except Exception:
        logging.exception("FATAL: consumer crashed")
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down...")
    finally:
        try:
            channel.close()
        finally:
            connection.close()
        sys.exit(0)
