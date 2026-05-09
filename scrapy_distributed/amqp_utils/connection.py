#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pika import URLParameters
from pika.adapters.blocking_connection import BlockingConnection


def get_channel(
    connection,
    queue,
    passive=False,
    durable=False,
    exclusive=False,
    auto_delete=False,
    arguments=None,
    exchange=None,
    exchange_type="direct",
    exchange_durable=True,
    exchange_arguments=None,
):
    """ Init method to return a prepared channel for consuming
    """
    channel = connection.channel()
    channel.queue_declare(
        queue=queue,
        passive=passive,
        durable=durable,
        exclusive=exclusive,
        auto_delete=auto_delete,
        arguments=arguments,
    )
    if exchange:
        channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            durable=exchange_durable,
            arguments=exchange_arguments,
        )
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=queue)
    # channel.confirm_delivery()
    return channel


def connect(connection_url):
    """ Create and return a fresh connection
    """
    return BlockingConnection(URLParameters(connection_url))
