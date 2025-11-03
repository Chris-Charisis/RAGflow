import pika
import json
import logging

from embedder.embedder.embedder import EmbeddingError
from .settings import settings

def publish_chunk(channel, exchange, routing_key, msg: dict):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        mandatory=True,
        body=json.dumps(msg),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
        ),
    )

def process_message(channel, method, properties, body, * embedder):
    try:
        payload = json.loads(body)
    except Exception:
        logging.error("Invalid JSON on input: %s", e)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        out_msg = embedder.process_message(payload)
        if not out_msg:
            logging.warning("No chunks produced; acking message")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return        
    except EmbeddingError as e:
        logging.error("Embedding failed; rejecting: %s", e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    except Exception as e:
        logging.error("Processing failed; rejecting: %s", e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    try:
        publish_chunk(
            channel,
            exchange=settings.rabbitmq_output_exchange,
            routing_key=settings.rabbitmq_output_routing_key,
            msg=out_msg,
        )
    except Exception as e:
        logging.error("Publish failed; rejecting: %s", e)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(
        "Embedding published at rk=%s",
        settings.rabbitmq_output_routing_key,
    )