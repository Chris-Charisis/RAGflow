import re
from minio import Minio
from minio.error import S3Error
from typing import Any, Iterable, Optional
import json, logging, tempfile, time
from pypdf import PdfReader
from pypdf.errors import PdfReadError
import pymupdf4llm
from .settings import settings
from .clients.rabbitmq_client import init_rabbitmq
from .helpers import (
    publish,
    is_already_processed,
    write_processed_marker,
    fetch_with_retry,
    parse_marker,
)
from pika.exceptions import (
    AMQPConnectionError,
    ChannelClosedByBroker,
    ConnectionClosed,
    ConnectionClosedByBroker,
    UnroutableError,
    NackError,
)

# Major section headings that become ===marker=== breaks for the chunker's
# boilerplate-stripping / chapter-aware strategies.
SECTION_NAMES = {
    "abstract", "introduction", "background", "related work",
    "materials and methods", "theory", "methods", "methodology",
    "experiments", "results", "results and discussion",
    "discussion", "conclusion", "conclusions",
    "acknowledgments", "references", "experimental section",
}

# Strip leading section numbering such as "4.3. Results" or "1. Introduction".
SECTION_NUM_PREFIX = re.compile(
    r"""
    ^\s*
    (?:
        \(?\d+(?:\.\d+)*               # 1 or 1.2.3
        |
        \(?
        [ivxlcdm]+(?:\.[ivxlcdm]+)*    # I, II, III, IV, ...
        (?=\W|$)
    )
    \)?
    [\.\)]?
    \s*
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

# Markdown table separator / rule lines like "|---|:--:|" or "----".
_MD_RULE = re.compile(r"^[\s|:\-]+$")


def _normalize_heading(text: str) -> str:
    """Strip section numbering and lowercase, for SECTION_NAMES matching."""
    return SECTION_NUM_PREFIX.sub("", text).strip().lower()


class PDFReader:
    """
    Service to read PDFs from MinIO, extract text and metadata, and publish to RabbitMQ.
    Uses processed markers to ensure idempotency and avoid reprocessing.
    Also scans for deletions and publishes deletion events.
    """

    # Initialize with MinIO client, RabbitMQ channel, and settings.
    def __init__(
        self,
        minio_client: Minio,
        *,
        bucket: Optional[str] = None,
        processed_prefix: Optional[str] = None,
        ingest_exchange: Optional[str] = None,
        ingest_routing_key: Optional[str] = None,
        delete_exchange: Optional[str] = None,
        delete_routing_key: Optional[str] = None,
        workers: Optional[int] = None,
    ) -> None:
 
        try:
            connection, channel = init_rabbitmq(settings)
        except Exception as e:
            logging.error("RabbitMQ client initialization failed: %s", e)
            raise
 
        self.client = minio_client
        self.connection = connection
        self.channel = channel

        # Bind from settings by default to avoid changes elsewhere
        self.bucket = bucket or settings.minio_bucket
        self.processed_prefix = processed_prefix or settings.processed_prefix

        self.ingest_exchange = ingest_exchange or settings.rabbitmq_exchange
        self.ingest_routing_key = ingest_routing_key or settings.rabbitmq_routing_key

        self.delete_exchange = delete_exchange or settings.rabbitmq_delete_exchange
        self.delete_routing_key = delete_routing_key or settings.rabbitmq_delete_routing_key

        self.workers = workers or settings.workers

    # Extract text (via PyMuPDF4LLM) and metadata (via pypdf) from a PDF.
    def extract_text_from_pdf(self, path) -> dict[str, Any]:
        """Extract metadata and text from a PDF file.

        Text is extracted with PyMuPDF4LLM as Markdown (fast, no torch/OCR),
        then markdown headings matching known section names are converted into
        the ===section=== markers the chunker uses. Images/figures are dropped;
        table text is kept.
        """
        out: dict[str, Any] = {}

        # ---- Metadata (pypdf) ----
        try:
            rdr = PdfReader(str(path))
        except PdfReadError as e:
            logging.error("PdfReadError for %s: %s", path, e)
            raise
        meta = rdr.metadata or {}

        def _meta(attr: str, label: str):
            try:
                return getattr(meta, attr)
            except (AttributeError, KeyError):
                logging.warning("%s not found in PDF metadata for %s", label, path)
                return None

        doi = None
        try:
            doi = self.guess_doi(rdr)
        except Exception as e:
            logging.warning("Failed to extract DOI from PDF %s: %s", path, e)

        out["metadata"] = {
            "title": _meta("title", "Title"),
            "authors": _meta("author", "Authors"),
            "keywords": _meta("keywords", "Keywords"),
            "abstract": _meta("subject", "Abstract/subject"),
            "doi": doi,
        }

        # ---- Text (PyMuPDF4LLM -> Markdown -> ===section=== markers) ----
        markdown = pymupdf4llm.to_markdown(str(path))
        out["text"] = self._markdown_to_marked_text(markdown)
        return out

    # Convert PyMuPDF4LLM markdown into the ===section=== delimited text the
    # chunker expects. Major-section headings become markers; other headings
    # and body text are kept; image refs and table-rule lines are dropped.
    @staticmethod
    def _markdown_to_marked_text(markdown: str) -> str:
        parts: list[str] = []
        for line in markdown.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("!["):  # markdown image reference
                continue
            if _MD_RULE.match(stripped):  # table separator / horizontal rule
                continue
            if stripped.startswith("#"):  # heading
                heading = stripped.lstrip("#").strip()
                normalized = _normalize_heading(heading)
                if normalized in SECTION_NAMES:
                    parts.append(f"\n\n==={normalized}===\n\n")
                else:
                    parts.append(heading)
                continue
            parts.append(stripped)

        raw_text = " ".join(parts)
        return re.sub(r"\s+", " ", raw_text).strip()

    # Guess DOI from the first two pages of the PDF
    def guess_doi(self, rdr):
        """Heuristic: search first two pages for a DOI‑looking pattern
        (10.xxxx/...). Returns the first match or None."""    
        for page in rdr.pages[:2]:
            txt = page.extract_text() or ""
            m = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", txt, re.I)
            if m:
                return m.group(0)
        return None

    # Process a single MinIO object: download, parse, publish, mark.
    # Skips publishing/marking if already processed.
    # Raises on any failure to allow caller to log and record.
    def process_object(self, obj):

        # Download to a temp file with retries
        with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
            try:
                fetch_with_retry(self.client, self.bucket, obj.object_name, tmp.name)
            except (S3Error, ConnectionError) as e:
                logging.warning("Giving up on %s after 4 retries: %s", obj.object_name, e)
                logging.exception("Download failed for '%s'", obj.object_name)
                raise
            
            # Main function to extract text and metadata
            try:
                processed_pdf_info = self.extract_text_from_pdf(tmp.name)
            except Exception as e:
                logging.error("Failed to process PDF %s: %s", obj.object_name, e)
                raise
            
            # Create message payload and publish to RabbitMQ
            try:
                payload: dict[str, Any] = {
                    "schema": 2,
                    "event": "ingest",
                    # Millisecond timestamp carried through the whole pipeline so the
                    # vector_indexer can resolve the "deleted while in flight" race.
                    "published_at": int(time.time() * 1000),
                    "source": {
                        "bucket": self.bucket,
                        "object": obj.object_name,
                        "etag": obj.etag,
                    }
                }
                payload.update(processed_pdf_info)

                # with open(f"./pdf_reader/outputs/{payload['source']['object'].replace('.pdf', '')}_processed.json", "w") as f:
                #     json.dump(payload, f, indent=4)

                # with open(f"./pdf_reader/outputs/{obj.object_name.replace('.pdf', '')}_processed.json", "r") as f:
                #     payload = json.load(f)

                self.connection, self.channel = publish(
                    self.connection,
                    self.channel,
                    self.ingest_exchange,
                    self.ingest_routing_key,
                    payload,
                    message_id=obj.etag,
                )

                # Mark as processed only after successful publish
                write_processed_marker(self.client, self.bucket, self.processed_prefix, obj)
                logging.info("Published & marked %s", obj.object_name)
            
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
    # Skips objects that already have a processed marker.
    def process_bucket(self, *, failed_log_path: Optional[str] = None, retry_file: Optional[str] = None) -> None:
        # List all objects in the bucket
        objects = list(self.client.list_objects(self.bucket, recursive=True))

        # If a retry file is provided, filter to only those objects
        if retry_file:
            with open(retry_file, "r") as fh:
                keys = [line.strip() for line in fh if line.strip()]
            objects = [self.client.stat_object(self.bucket, k) for k in keys]
            if not objects:
                logging.warning("No failed objects found in retry file: '%s'", retry_file)
                return
            logging.info("Retry mode: processing %d failed objects", len(objects))
        else:
            objects = list(self._list_source_objects())
            if not objects:
                logging.info("No objects found in bucket: '%s'", self.bucket)

        # Process each object
        processed_count = 0
        for obj in objects:
            # skip if already processed
            if is_already_processed(self.client, self.bucket, self.processed_prefix, obj):
                continue
            try:
                self.process_object(obj)
                processed_count += 1
            except Exception as e:
                logging.error("Processing failed for '%s': %s", obj.object_name, e)
                if failed_log_path:
                    with open(failed_log_path, "a", encoding="utf-8") as fh:
                        fh.write(obj.object_name + "\n")

        if processed_count:
            logging.info("Processed %d new file(s)", processed_count)     


    # List all non-marker objects from the bucket.
    def _list_source_objects(self) -> Iterable:
        objs = self.client.list_objects(self.bucket, recursive=True)
        # Filter out marker objects
        for o in objs:
            if not o.object_name.startswith(self.processed_prefix + "/"):
                yield o            

    # Scan for deletions by checking markers whose source objects no longer exist.
    # Publishes one deletion event per unique object key and removes all associated markers.
    def scan_deletions(self) -> None:

        # List all marker objects once
        markers = list(
            self.client.list_objects(
                self.bucket, prefix=self.processed_prefix + "/", recursive=True
            )
        )

        removed_count = 0
        # Ensure one deletion event per key
        published_keys: set[str] = set()  

        # Iterate over markers to find stale ones
        for m in markers:
            marker_path = m.object_name
            # Parse to get original key and etag
            try:
                src_key, etag = parse_marker(self.processed_prefix, marker_path)
            except Exception:
                logging.warning("Skipping unknown marker format: %s", marker_path)
                continue

            # If we've already handled this key in this sweep, skip quickly.
            if src_key in published_keys:
                continue

            # Check if the source still exists
            missing = False
            try:
                self.client.stat_object(self.bucket, src_key)
            except S3Error as e:
                # Missing source => proceed, otherwise log and continue
                if getattr(e, "code", "") in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                    missing = True
                else:
                    logging.debug("stat_object error for %s: %s", src_key, e)
            
            # Still exists, keep markers
            if not missing:
                continue  

            # Publish a single deletion event for this key (use current marker's etag)
            deletion_msg = {
                "schema": 2,
                "event": "deletion",
                # Any chunk published at or before this instant belongs to the
                # deleted generation and must be removed / rejected downstream.
                "deleted_at": int(time.time() * 1000),
                "source": {"bucket": self.bucket, "object": src_key, "etag": etag},
            }
            try:
                publish(
                    self.connection,
                    self.channel,
                    self.delete_exchange,
                    self.delete_routing_key,
                    deletion_msg,
                    message_id=f"del-{etag}",
                )
            except Exception as e:
                logging.error("Failed to publish deletion for %s: %s", src_key, e)
                # Don't remove markers if we couldn't notify downstream
                continue

            # Remove ALL historical markers for this src_key (every etag)
            # We filter to names that start exactly with "<prefix>/<src_key>."
            # to avoid accidental prefix collisions like "a/b" vs "a/bb".
            prefix_for_key = f"{self.processed_prefix}/{src_key}"
            try:
                siblings = self.client.list_objects(
                    self.bucket, prefix=prefix_for_key, recursive=False
                )
                for sib in siblings:
                    if not sib.object_name.startswith(prefix_for_key + "."):
                        continue
                    try:
                        self.client.remove_object(self.bucket, sib.object_name)
                        removed_count += 1
                        logging.info("Removed marker: %s", sib.object_name)
                    except Exception as e:
                        logging.error("Failed to remove marker %s: %s", sib.object_name, e)
            except Exception as e:
                logging.error("Failed to list markers for %s: %s", src_key, e)

            # Record that we've already published deletion for this key
            published_keys.add(src_key)

        if removed_count:
            logging.info("Deletion sweep: removed %d stale marker(s)", removed_count)
           

    def close_rabbitqm(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception as e:
            logging.warning("Error closing RabbitMQ channel: %s", e)
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            logging.warning("Error closing RabbitMQ connection: %s", e)