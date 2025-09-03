import pika
from ..settings import Settings

def init_rabbitmq(cfg: Settings):
    credentials = pika.PlainCredentials(cfg.rabbitmq_user, cfg.rabbitmq_password)
    params = pika.ConnectionParameters(
        host=cfg.rabbitmq_url,
        credentials=credentials,
        heartbeat=120,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Input setup
    channel.exchange_declare(exchange=cfg.input_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=cfg.input_queue, durable=True)
    channel.queue_bind(queue=cfg.input_queue, exchange=cfg.input_exchange, routing_key=cfg.input_routing_key)

    # Output setup
    channel.exchange_declare(exchange=cfg.output_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=cfg.output_queue, durable=True)
    channel.queue_bind(queue=cfg.output_queue, exchange=cfg.output_exchange, routing_key=cfg.output_routing_key)

    channel.confirm_delivery()
    return connection, channel