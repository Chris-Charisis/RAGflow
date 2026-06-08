from ragflow_contracts.mq import connect, declare
from ..settings import Settings


def init_rabbitmq(cfg: Settings):
    connection, channel = connect(cfg)
    declare(channel, cfg.rabbitmq_input_exchange, cfg.rabbitmq_input_queue, cfg.rabbitmq_input_routing_key)
    declare(channel, cfg.rabbitmq_output_exchange, cfg.rabbitmq_output_queue, cfg.rabbitmq_output_routing_key)
    return connection, channel
