#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pika

from scrapy_distributed.amqp_utils import connection

logger = logging.getLogger(__name__)


class QueueConfig(object):
    def __init__(
        self,
        name,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
    ):
        self.name = name
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments


class IQueue(object):
    """Per-spider queue/stack base class"""

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, url):
        """Push an url"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop an url"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        raise NotImplementedError


def _try_operation(function):
    """Wrap unary method by reconnect procedure"""

    def wrapper(self, *args, **kwargs):
        retries = 0
        while retries < 10:
            try:
                return function(self, *args, **kwargs)
            except Exception as e:
                retries += 1
                msg = "Function %s failed. Reconnecting... (%d times)" % (
                    str(function),
                    retries,
                )
                logger.error(msg)
                self.connect()
                time.sleep((retries - 1) * 5)
                raise e
        return None

    return wrapper


class RabbitQueue(IQueue):
    """Per-spider FIFO queue"""

    def __init__(
        self,
        connection_url,
        name,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
    ):
        """Initialize per-spider RabbitMQ queue.

        Parameters:
            connection_url -- rabbitmq connection url
            key -- rabbitmq routing key
        """
        self.name = name
        self.connection_url = connection_url
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.connection = None
        self.channel = None
        self.connect()

    @classmethod
    def from_queue_conf(cls, connection_url, queue_conf: QueueConfig):
        return cls(
            connection_url,
            name=queue_conf.name,
            passive=queue_conf.passive,
            durable=queue_conf.durable,
            exclusive=queue_conf.exclusive,
            auto_delete=queue_conf.auto_delete,
            arguments=queue_conf.arguments,
        )

    def __len__(self):
        """Return the length of the queue"""
        declared = self.channel.queue_declare(
            self.name,
            passive=self.passive,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete,
            arguments=self.arguments,
        )
        return declared.method.message_count

    @_try_operation
    def pop(self, auto_ack=False):
        """Pop a message"""
        return self.channel.basic_get(queue=self.name, auto_ack=auto_ack)

    @_try_operation
    def ack(self, delivery_tag):
        """Ack a message"""
        self.channel.basic_ack(delivery_tag=delivery_tag)

    @_try_operation
    def push(self, body, headers=None, exchange=""):
        """Push a message"""
        properties = None
        if headers:
            properties = pika.BasicProperties(headers=headers)
        self.channel.basic_publish(
            exchange=exchange, routing_key=self.name, body=body, properties=properties
        )

    @_try_operation
    def ack(self, delivery_tag):
        """Ack a message"""
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def connect(self):
        """Make a connection"""
        logger.info(f"connect AMQP: {self.connection_url}")
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        self.connection = connection.connect(self.connection_url)
        self.channel = connection.get_channel(
            connection=self.connection,
            queue=self.name,
            passive=self.passive,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete,
            arguments=self.arguments,
        )

    def close(self):
        """Close channel"""
        logger.error(f"close AMQP connection")
        self.channel.close()

    def clear(self):
        """Clear queue/stack"""
        self.channel.queue_purge(self.name)


__all__ = ["RabbitQueue"]
