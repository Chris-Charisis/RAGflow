import argparse
from functools import partial
import json
import logging
import sys
# from dotenv import load_dotenv

from .settings import settings
from .clients.rabbitmq_client import init_rabbitmq
from .clients.ollama_client import OllamaClient
from .embedder import Embedder
from .helpers import process_message

# load_dotenv()


def init_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def main() -> None:
    init_logging()
    parser = argparse.ArgumentParser(
        description="RAGflow Embedder: consume chunks, embed with Ollama, republish."
    )
    parser.add_argument("--model", help="Override OLLAMA_MODEL")
    parser.add_argument("--dimensions", type=int, help="Vector dims if supported")
    parser.add_argument("--timeout", type=int, help="Ollama request timeout (seconds)")
    args = parser.parse_args()

    logging.info("Initializing Ollama connection...")
    # Build core objects (OOP style)
    ollama = OllamaClient(
        base_url=settings.ollama_base_url,
        timeout_s=args.timeout or settings.ollama_timeout_seconds,
    )

    logging.info("Initializing Embedder...")
    embedder = Embedder(
        client=ollama,
        model=args.model or settings.ollama_model,
        dimensions=args.dimensions or settings.ollama_dimensions,
        truncate=True,
    )

    logging.info("Initializing RabbitMQ client...")
    try:
        # Establish RabbitMQ connection
        connection, channel = init_rabbitmq(settings)
    except Exception as e:
        logging.error("RabbitMQ init failed: %s", e)
        raise

    try:
        # Expand callback with partial to include arguments variable and be compatible with basic_consume
        cb = partial(process_message, embedder=embedder)
        channel.basic_consume(
            queue=settings.rabbitmq_input_queue,
            on_message_callback=cb,
            auto_ack=False,
        )

        logging.info("Consuming from %s with routing key %s",
                     settings.rabbitmq_input_queue, settings.rabbitmq_input_routing_key)

        channel.start_consuming()        
    except BaseException as e:
        # Catch broader-than-Exception (e.g., SystemExit from libraries)
        logging.exception("Unexpected fatal error; continuing: %s", e)
        # time.sleep(poll)
    except KeyboardInterrupt:
        logging.info("Interrupted; shutting down...")
    finally:
        try:
            channel.close()
        finally:
            connection.close()
        sys.exit(0)
