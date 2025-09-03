# Main module for extracting text and metadata from PDF files stored in MinIO
# and publishing the results to RabbitMQ.
from typing import Any
import json, re, logging, tempfile, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from minio import Minio
from minio.error import S3Error
from pika.adapters.blocking_connection import BlockingChannel
from pypdf import PdfReader
import pdfplumber
from unstructured.partition.pdf import partition_pdf

import pika
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosed,
    ConnectionClosedByBroker,
    UnroutableError,
    NackError,
)

# Guess DOI from the first two pages of the PDF
def _guess_doi(rdr):
    """Heuristic: search first two pages for a DOI‑looking pattern
    (10.xxxx/...). Returns the first match or None."""    
    for page in rdr.pages[:2]:
        txt = page.extract_text() or ""
        m = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", txt, re.I)
        if m:
            return m.group(0)
    return None

# Extract metadata and text from a PDF file
def extract_text_from_pdf(path):
    """ Extract metadata and text from a PDF file. Images, tables, and other non-text elements are ignored."""
    
    # Extract metadata
    out = {}
    rdr = PdfReader(str(path))
    meta = rdr.metadata or {}
    out["metadata"] = {
        "title": meta.title,
        "authors": meta.author,
        "keywords": meta.keywords,
        "abstract": meta.subject,
        "doi": _guess_doi(rdr),
    }

    # Extract raw pages text
    with pdfplumber.open(path) as pdf:
        raw_pages = [p.extract_text(x_tolerance=1.5) or "" for p in pdf.pages]
    raw_text = "\n\n".join(raw_pages)

    # Process the PDF to extract structured elements like titles, paragraphs, etc.
    # This will also handle page breaks and table structures.
    elts = partition_pdf(
        filename=str(path),
        strategy="hi_res",
        infer_table_structure=True,
        include_page_breaks=False,
        form_extraction_skip_tables=False,
    )

    # Define a function to filter out major section headers and narrative text
    SECTION_NAMES = {
        "abstract", "introduction", "background", "related work",
        "materials and methods", "theory", "methods", "methodology",
        "experiments", "results", "results and discussion",
        "discussion", "conclusion", "conclusions",
        "acknowledgments", "references", "experimental section"
    }

    # Precompile once
    SECTION_NUM_PREFIX = re.compile(
        r"""^
            \s*
            (?:                     # numbering alternatives:
            \(?\d+(?:\.\d+)*     # 1  or 1.2.3   (optional opening '(')
            |                     # OR
            \(?[ivxlcdm]+(?:\.[ivxlcdm]+)*  # I  or IV or III      (roman)
            )
            \)?                     # optional closing ')'
            [\.\)]?                 # optional trailing '.' or ')'
            \s*                     # spaces after numbering
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    # Strip leading numbers from section titles such as “4.3. Results” or “1. Introduction”
    def is_major_header(text: str) -> str:
        clean = SECTION_NUM_PREFIX.sub("", text).strip().lower()
        return clean

    filtered = []
    for el in elts:
        if el.category == "NarrativeText":
            if len(el.text) > 150 or el.text.startswith("Figure ") or el.text.startswith("Table "): 
                filtered.append(el.text)
        elif el.category == "Title":
            stripped_title = is_major_header(el.text)
            # mark real section breaks with a flag so we can split the text later
            if stripped_title in SECTION_NAMES:
                filtered.append(f"\n\n==={stripped_title}===\n\n")

    # Concatenate and normalise whitespaces
    raw_text = " ".join(filtered)
    clean_text = re.sub(r"\s+", " ", raw_text).strip()
    out["text"] = clean_text

    return out

# Publish a JSON payload to RabbitMQ and surface network or AMQP exceptions so the caller can record failures.
def publish(channel: BlockingChannel, exchange: str, routing_key: str, msg: dict[str, Any]) -> None:
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(msg),
        mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent
        ),
    )

# Wrapper around MinIO `fget_object` that retries with
# exponential backoff on transient network errors.
@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, max=8),
    retry=retry_if_exception_type(S3Error),
    reraise=True
)
def fetch_with_retry(client, bucket, obj_name, dest):
    client.fget_object(bucket, obj_name, dest)
    logging.info(f"✓ Downloaded {obj_name}")

# Process a single object from MinIO: download it, extract text and metadata, and publish to RabbitMQ.
# Any failure bubbles up so `process_bucket` can mark the key failed.
def process_object(obj, client: Minio, bucket: str, channel: BlockingChannel, exchange: str, routing_key: str):
    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
        try:
            fetch_with_retry(client, bucket, obj.object_name, tmp.name)
        except (S3Error, ConnectionError) as e:
            logging.warning("Giving up on %s after 4 retries: %s", obj.object_name, e)
            logging.exception("Download failed for '%s'", obj.object_name)
            raise
        
        try:
            processed_pdf_info = extract_text_from_pdf(tmp.name)
        except Exception as e:
            logging.error("Failed to process PDF %s: %s", obj.object_name, e)
            raise
        
        try:
            payload: dict[str, Any] = {
                "schema": 1,
                "source": {
                    "bucket": bucket,
                    "object": obj.object_name,
                    "etag": obj.etag,
                }     
            }
            payload.update(processed_pdf_info)

            with open(f"./pdf_reader/outputs/{payload["source"]["object"]}_processed.json", "w") as f:
                json.dump(payload, f, indent=4)

            publish(channel, exchange, routing_key, payload)

            logging.info("Published %s", obj.object_name)
        
        except Exception:
            logging.exception("FATAL: consumer crashed")
        except (
            AMQPConnectionError,
            ChannelClosedByBroker,
            ConnectionClosed,
            ConnectionClosedByBroker,
            UnroutableError,
            NackError,
        ) as e:
            logging.error("RabbitMQ publish failed for %s with error: %s", obj.object_name, e)
            raise

# Process a bucket of objects from MinIO, downloading each PDF, extracting text and metadata, and publishing to RabbitMQ.
# If `retry_file` is provided, only process objects listed in that file.
def process_bucket(client: Minio, bucket: str, channel: BlockingChannel, exchange: str, routing_key: str, workers: int = 4, failed_log_path: str | None = None, retry_file: str | None = None) -> None:
    objects = list(client.list_objects(bucket, recursive=True))

    if retry_file:
        with open(retry_file) as fh:
            keys = [line.strip() for line in fh if line.strip()]
        objects = [client.stat_object(bucket, k) for k in keys]
        if not objects:
            logging.warning("No failed objects found in retry file: '%s'", retry_file)
            return
        logging.info("Retry mode: processing %d failed objects", len(objects))
    else:
        objects = list(client.list_objects(bucket, recursive=True))
        if not objects:
            logging.warning("No objects found in bucket: '%s'", bucket)
            return    

    from contextlib import nullcontext
    log_ctx = open(failed_log_path, "a") if failed_log_path else nullcontext()

    with log_ctx as fail_fh:
    
        for obj in objects:
            try:
                process_object(obj, client, bucket, channel, exchange, routing_key)
            except Exception:
                logging.error("Processing failed for '%s'", obj.object_name)
                if failed_log_path:
                    fail_fh.write(obj.object_name + "\n")            

        logging.info("Finished – processed %s file(s)", len(objects))

        # with ThreadPoolExecutor(max_workers=workers) as executor:
        #     futures = {
        #         executor.submit(
        #             process_object, obj, client, bucket, channel, exchange, routing_key
        #         ): obj
        #         for obj in objects
        #     }
        #     for fut in as_completed(futures):
        #         obj = futures[fut]
        #         try:
        #             fut.result()
        #         except Exception:
        #             logging.error("Failed processing %s", obj.object_name)
        #             if failed_log_path:
        #                 fail_fh.write(obj.object_name + "\\n")       
        # logging.info("Finished – processed %s file(s)", len(objects))
        # sys.exit(0)

