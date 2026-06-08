from ragflow_contracts.mq import connect, declare
from ..settings import Settings


def init_rabbitmq(cfg: Settings):
    connection, channel = connect(cfg)
    # Primary 'text' events + 'deletion' events.
    declare(channel, cfg.rabbitmq_exchange, cfg.rabbitmq_queue, cfg.rabbitmq_routing_key)
    declare(channel, cfg.rabbitmq_delete_exchange, cfg.rabbitmq_delete_queue, cfg.rabbitmq_delete_routing_key)
    return connection, channel
