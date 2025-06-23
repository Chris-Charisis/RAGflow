import pika
from ..settings import Settings

def init_rabbitmq(cfg: Settings):
    credentials = pika.PlainCredentials(cfg.rabbitmq_user, cfg.rabbitmq_password)
    parameters = pika.ConnectionParameters(host=cfg.rabbitmq_url, credentials=credentials, heartbeat=120, blocked_connection_timeout=300)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()

    channel.exchange_declare(
        exchange=cfg.rabbitmq_exchange, exchange_type="topic", durable=True
    )
    channel.exchange_declare(exchange=cfg.rabbitmq_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=cfg.rabbitmq_queue, durable=True)
    channel.queue_bind(queue=cfg.rabbitmq_queue, exchange=cfg.rabbitmq_exchange, routing_key=cfg.rabbitmq_routing_key)

    channel.confirm_delivery()
    return connection, channel
