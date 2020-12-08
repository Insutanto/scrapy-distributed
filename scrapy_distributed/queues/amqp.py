#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from typing import Dict

from scrapy.http.request import Request
from scrapy_distributed.queues.common import BytesDump, keys_string
from scrapy.utils.misc import load_object

from scrapy.utils.reqser import _get_method, request_to_dict
from w3lib.util import to_unicode
from scrapy_distributed.queues import IQueue
import time

import pika

from scrapy_distributed.amqp_utils import connection
from scrapy_distributed.common.queue_config import RabbitQueueConfig

logger = logging.getLogger(__name__)


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
        connection_url: str,
        name: str,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
        properties=None
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
        self.properties = properties
        self.connection = None
        self.channel = None
        self.connect()

    @classmethod
    def from_queue_conf(cls, connection_url, queue_conf: RabbitQueueConfig):
        return cls(
            connection_url,
            name=queue_conf.name,
            passive=queue_conf.passive,
            durable=queue_conf.durable,
            exclusive=queue_conf.exclusive,
            auto_delete=queue_conf.auto_delete,
            arguments=queue_conf.arguments,
            properties = queue_conf.properties
        )

    def __len__(self):
        """Return the length of the queue"""
        declared = self.channel.queue_declare(
            self.name,
            passive=self.passive,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete,
            arguments=self.arguments
        )
        return declared.method.message_count

    @_try_operation
    def pop(self, scheduler, auto_ack=False):
        """Pop a message"""
        method, properties, body = self.channel.basic_get(queue=self.name, auto_ack=auto_ack)
        if body is None:
            return None
        request = self._make_request(method, properties, body, scheduler)
        request.meta["delivery_tag"] = method.delivery_tag
        return request

    def _make_request(self, method, properties, body, scheduler):
        return self._request_from_dict(
            json.loads(body.decode()), scheduler.spider, method, properties
        )

    @_try_operation
    def push(self, request, scheduler, headers=None, exchange=""):
        """Push a message"""
        body = json.dumps(
                keys_string(self._request_to_dict(request, scheduler.spider)),
                cls=BytesDump,
            )
        if headers:
            properties = pika.BasicProperties(
                headers=headers, 
                delivery_mode=self.properties.get("delivery_mode", 1))
        else:
            properties = pika.BasicProperties(delivery_mode=self.properties.get("delivery_mode", 1))
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

    @classmethod
    def _request_from_dict(cls, d, spider=None, method=None, properties=None):
        """Create Request object from a dict.

        If a spider is given, it will try to resolve the callbacks looking at the
        spider for methods with the same name.
        """
        cb = d.get("callback", None)
        if cb and spider:
            cb = _get_method(spider, cb)
        eb = d.get("errback", None)
        if eb and spider:
            eb = _get_method(spider, eb)
        request_cls = load_object(d.get("_class", None)) if "_class" in d else Request
        return request_cls(
            url=to_unicode(d.get("url", None)),
            callback=cb,
            errback=eb,
            method=d.get("method", None),
            headers=d.get("headers", None),
            body=d.get("body", None),
            cookies=d.get("cookies", None),
            meta=d.get("meta", None),
            encoding=d.get("_encoding", None),
            priority=d.get("priority", 0),
            dont_filter=d.get("dont_filter", True),
            flags=d.get("flags", None),
        )

    @classmethod
    def _request_to_dict(cls, request, spider=None):
        d = request_to_dict(request, spider)
        new_dict = dict()
        for key, value in d.items():
            if value:
                new_dict[key] = value
        logger.debug(f"request_to_dict: {d}")
        return new_dict



__all__ = ["RabbitQueue"]
