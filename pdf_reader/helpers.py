import json
import pika
import io
import logging
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import ChannelWrongStateError, StreamLostError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Any
from minio import Minio
from minio.error import S3Error
from .settings import Settings
from .clients.rabbitmq_client import init_rabbitmq

# Publish a JSON payload to RabbitMQ and surface network or AMQP exceptions so the caller can record failures.
def publish(connection: BlockingConnection, channel: BlockingChannel, exchange: str, routing_key: str, msg: dict[str, Any], message_id: str | None = None) -> None:
    """
    Publish once; if the channel/stream is stale after long idle,
    reconnect and retry exactly once.
    """

    # Fast path: if channel looks open, try once.
    if channel and getattr(channel, "is_open", False):
        try:
            channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(msg), #.encode("utf-8")
            mandatory=True,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=message_id,
                ),
            )
            return connection, channel
        except (ChannelWrongStateError, StreamLostError):
            logging.warning("RabbitMQ channel/stream lost; reconnecting...")
            pass  # fall through to reconnect

    # Reconnect and retry once (handles hours/days of idle).
    cfg = Settings()
    connection, channel = init_rabbitmq(cfg)
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(msg), #.encode("utf-8")
        mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id,
            ),
        )
    return connection, channel

# -------- Processed marker helpers --------
def marker_key(processed_prefix: str, obj_key: str, etag: str) -> str:
    safe_key = obj_key.replace("\\", "/")
    return f"{processed_prefix}/{safe_key}.{etag}.done"

def parse_marker(processed_prefix: str, marker_path: str) -> tuple[str, str]:
    """
    Inverse of marker_key(): returns (original_key, etag).
    Accepts dots in original filenames.
    """
    if not marker_path.startswith(processed_prefix + "/"):
        raise ValueError("Marker path has unexpected prefix")
    tail = marker_path[len(processed_prefix) + 1 :]  # drop "<prefix>/"
    base, etag, done = tail.rsplit(".", 2)
    if done != "done":
        raise ValueError("Marker path missing '.done' suffix")
    return base, etag

def is_already_processed(client: Minio, bucket: str, processed_prefix: str, obj) -> bool:
    mk = marker_key(processed_prefix, obj.object_name, obj.etag)
    try:
        client.stat_object(bucket, mk)
        return True
    except S3Error:
        return False

def write_processed_marker(client: Minio, bucket: str, processed_prefix: str, obj) -> None:
    mk = marker_key(processed_prefix, obj.object_name, obj.etag)
    client.put_object(
        bucket,
        mk,
        io.BytesIO(b""),
        length=0,
        content_type="text/plain",
        metadata={"source": obj.object_name, "etag": obj.etag},
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