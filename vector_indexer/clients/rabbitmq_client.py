from ragflow_contracts.mq import connect, declare
from ..settings import Settings


def init_rabbitmq(cfg: Settings, *, prefetch=None):
    connection, channel = connect(cfg, prefetch=prefetch)
    declare(channel, cfg.rabbitmq_input_exchange, cfg.rabbitmq_input_queue, cfg.rabbitmq_input_routing_key)
    declare(channel, cfg.rabbitmq_delete_exchange, cfg.rabbitmq_delete_queue, cfg.rabbitmq_delete_routing_key)
    return connection, channel
