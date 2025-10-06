# Command-line interface for PDF reading
import argparse
from datetime import datetime
import time
import logging, sys
from dotenv import load_dotenv
load_dotenv()  
from .settings import settings
from .pdf_reader import PDFReader
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
    init_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Override bucket name")
    parser.add_argument("--failed-log",default="failed_objects.txt",help="Path to append objects that fail (default: %(default)s)")
    parser.add_argument("--retry-file",help="Path with one object key per line to retry instead of scanning bucket")
    parser.add_argument("--workers", type=int, help="Override worker count")
    parser.add_argument("--watch", action="store_true", help="Run forever, polling for new objects / deletions")
    parser.add_argument("--poll-interval", type=int, default=5, help="Seconds to sleep between scans (default from settings)")    
    # parser.add_argument("--routing-key", help="Override routing key")
    args = parser.parse_args()

    bucket = args.bucket or settings.minio_bucket
    workers = args.workers or settings.workers
    poll = args.poll_interval or settings.poll_interval_seconds

    # routing_key = args.routing_key or settings.rabbitmq_routing_key

    logging.info("Initializing clients...")
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

    # Service encapsulating the workflow (minimal change to the rest of the codebase)
    pdf_reader = PDFReader(
        client,
        channel,
        bucket=bucket,
        processed_prefix=settings.processed_prefix,
        ingest_exchange=settings.rabbitmq_exchange,
        ingest_routing_key=settings.rabbitmq_routing_key,
        delete_exchange=settings.rabbitmq_delete_exchange,
        delete_routing_key=settings.rabbitmq_delete_routing_key,
        workers=workers,
    )

    logging.info("Job started at %s", datetime.utcnow().isoformat())

    print("Watch mode:", args.watch)

    try:
        if args.watch:
            logging.info("Watch mode ON. Poll interval: %ss", poll)
            while True:
                try:
                    logging.debug("---- loop start ----")
                    pdf_reader.process_bucket(
                        failed_log_path=args.failed_log,
                        retry_file=args.retry_file,
                    )
                    pdf_reader.scan_deletions()
                    logging.debug("---- loop end; sleeping %ss ----", poll)
                    time.sleep(poll)
                except KeyboardInterrupt:
                    logging.info("KeyboardInterrupt: stopping watch loop.")
                    break
                except BaseException as e:
                    # Catch broader-than-Exception (e.g., SystemExit from libraries)
                    logging.exception("Unexpected fatal error in loop; continuing: %s", e)
                    time.sleep(poll)
        else:
            pdf_reader.process_bucket(
                failed_log_path=args.failed_log,
                retry_file=args.retry_file,
            )
            pdf_reader.scan_deletions()
    finally:
        # Graceful shutdown without sys.exit(0); let caller/process decide exit code
        try:
            try:
                channel.close()
            finally:
                connection.close()
        except Exception as e:
            logging.warning("Error during shutdown: %s", e)