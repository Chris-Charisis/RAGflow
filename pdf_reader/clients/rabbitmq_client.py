import pika
from ..settings import Settings

def init_rabbitmq(cfg: Settings):
    # Set up RabbitMQ connection and channel
    credentials = pika.PlainCredentials(cfg.rabbitmq_user, cfg.rabbitmq_password)
    parameters = pika.ConnectionParameters(
        host=cfg.rabbitmq_host,
        port=cfg.rabbitmq_port,
        virtual_host=cfg.rabbitmq_vhost,
        credentials=credentials,
        heartbeat=120,
        blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()

    # Declare primary exchange/queue for processed 'ingest' events
    channel.exchange_declare(exchange=cfg.rabbitmq_exchange, exchange_type="topic", durable=True)
    if cfg.rabbitmq_queue:
        channel.queue_declare(queue=cfg.rabbitmq_queue, durable=True)
        channel.queue_bind(queue=cfg.rabbitmq_queue, exchange=cfg.rabbitmq_exchange, routing_key=cfg.rabbitmq_routing_key)

    # Declare deletion exchange/queue (can be same exchange with different routing key)
    channel.exchange_declare(exchange=cfg.rabbitmq_delete_exchange, exchange_type="topic", durable=True)
    if cfg.rabbitmq_delete_queue:
        channel.queue_declare(queue=cfg.rabbitmq_delete_queue, durable=True)
        channel.queue_bind(queue=cfg.rabbitmq_delete_queue, exchange=cfg.rabbitmq_delete_exchange, routing_key=cfg.rabbitmq_delete_routing_key)

    channel.confirm_delivery()
    return connection, channel
