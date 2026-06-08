import pika
from ..settings import Settings

def init_rabbitmq(cfg: Settings):
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

    # RabbitMQ input setup for incoming 'embeddings' events
    channel.exchange_declare(exchange=cfg.rabbitmq_input_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=cfg.rabbitmq_input_queue, durable=True)
    channel.queue_bind(queue=cfg.rabbitmq_input_queue, exchange=cfg.rabbitmq_input_exchange, routing_key=cfg.rabbitmq_input_routing_key)

    # RabbitMQ setup for incoming 'deletion' events
    channel.exchange_declare(exchange=cfg.rabbitmq_delete_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=cfg.rabbitmq_delete_queue, durable=True)
    channel.queue_bind(queue=cfg.rabbitmq_delete_queue, exchange=cfg.rabbitmq_delete_exchange, routing_key=cfg.rabbitmq_delete_routing_key)

    channel.confirm_delivery()
    # Set how many messages are prefetched before they start being consumed
    channel.basic_qos(prefetch_count=cfg.rabbitmq_prefetch_count)
    return connection, channel