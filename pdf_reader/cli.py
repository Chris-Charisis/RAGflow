# Command-line interface for PDF reading
import argparse
from datetime import datetime
import logging, sys
from dotenv import load_dotenv
load_dotenv()  
from .settings import settings
from .pdf_reader import process_bucket
from .clients.minio_client import init_minio
from .clients.rabbitmq_client import init_rabbitmq

# Configure a JSON‑friendly stream handler using the log level retrieved from Settings.
# Kept in its own function so tests can call it without executing the rest of the CLI logic.
def init_logging():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Parse command‑line flags, wire dependencies (MinIO & RabbitMQ clients),
# then hand control to :pyfunc:`pdf_reader.pdf_reader.process_bucket`.
# Mainly exists so the package can be used both as a library and as a script.
def main()-> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Override bucket name")
    parser.add_argument("--failed-log",default="failed_objects.txt",help="Path to append objects that fail (default: %(default)s)")
    parser.add_argument("--retry-file",help="Path with one object key per line to retry instead of scanning bucket")
    parser.add_argument("--workers", type=int, help="Override worker count")
    # parser.add_argument("--routing-key", help="Override routing key")
    args = parser.parse_args()

    bucket = args.bucket or settings.minio_bucket
    workers = args.workers or settings.workers
    # routing_key = args.routing_key or settings.rabbitmq_routing_key

    init_logging()
    try:
        client = init_minio(settings)
    except Exception as e:
        logging.error("MinIO client initialization failed: %s", e)
        raise

    try:
        if not client.bucket_exists(bucket):
            raise RuntimeError(f"Bucket {bucket} does not exist")
    except Exception as e:
        logging.error("MinIO startup failure: %s", e)
        raise

    try:
        connection, channel = init_rabbitmq(settings)
    except Exception as e:
        logging.error("RabbitMQ client initialization failed: %s", e)
        raise

    logging.info("Job started at %s", datetime.utcnow().isoformat())

    try:
        process_bucket(
            client,
            bucket=bucket,
            channel=channel,
            exchange=settings.rabbitmq_exchange,
            routing_key=settings.rabbitmq_routing_key,
            workers=workers,
            failed_log_path=args.failed_log,
            retry_file=args.retry_file,
        )
    except KeyboardInterrupt:
        logging.info("Interrupted by user, shutting down...")        
    finally:
        try:
            channel.close()
        finally:
            connection.close()
        sys.exit(0)
