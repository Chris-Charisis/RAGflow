import pika
import json
import logging
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

def process_message(channel, method, properties, body, *, chunker):
    # Read message and acknowledge it
    try:
        payload = json.loads(body)
    except Exception as e:
        logging.error("Invalid JSON on input: %s", e)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        # Produce chunks based on the selected strategy
        out_msgs = chunker.chunk_payload(payload)
        if not out_msgs:
            logging.warning("No chunks produced; acking message")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Publish the chunks to RabbitMQ
        for m in out_msgs:
            publish_chunk(
                channel,
                exchange=settings.rabbitmq_output_exchange,
                routing_key=settings.rabbitmq_output_routing_key,
                msg=m,
            )
        logging.info("Published %d chunk(s)", len(out_msgs))
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        logging.exception("Processing failed; nacking without requeue")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)