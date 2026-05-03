#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import time
from queue import Empty, Queue

from scrapy.utils.request import request_from_dict

from rocketmq.client import ConsumeStatus, Message, Producer, PushConsumer

from scrapy_distributed.common.queue_config import RocketMQQueueConfig
from scrapy_distributed.queues import IQueue
from scrapy_distributed.queues.common import BytesDump, keys_string

logger = logging.getLogger(__name__)

_MAX_RETRIES = 10
_RETRY_SLEEP_SECONDS = 5


def _try_operation(function):
    """Wrap unary method by reconnect procedure"""

    def wrapper(self, *args, **kwargs):
        retries = 0
        last_exception = None
        while retries < _MAX_RETRIES:
            try:
                return function(self, *args, **kwargs)
            except Exception as e:
                retries += 1
                last_exception = e
                logger.error(
                    f"Function {function} failed. Reconnecting... ({retries} times)"
                )
                self.connect()
                time.sleep((retries - 1) * _RETRY_SLEEP_SECONDS)
        if last_exception:
            raise last_exception
        return None

    return wrapper


class RocketMQQueue(IQueue):
    """Per-spider RocketMQ FIFO queue.

    Uses a Producer to enqueue requests and a PushConsumer (with an internal
    thread-safe buffer) to dequeue them, mirroring the Kafka/RabbitMQ queue
    interfaces.
    """

    def __init__(
        self,
        name_server: str,
        topic: str,
        group: str = "default",
        tags: str = None,
        keys: str = None,
        arguments=None,
    ):
        """Initialise the RocketMQ queue.

        Parameters:
            name_server -- RocketMQ name-server address (``host:port``)
            topic       -- Topic used as the crawl queue
            group       -- Producer / consumer group name
            tags        -- Optional message tags for filtering
            keys        -- Optional message keys
            arguments   -- Reserved for future use
        """
        self.name_server = name_server
        self.name = topic
        self.topic = topic
        self.group = group
        self.tags = tags
        self.keys = keys
        self.arguments = arguments
        self._msg_buffer: Queue = Queue()
        self.producer = None
        self.consumer = None
        self.connect()

    @classmethod
    def from_queue_conf(cls, name_server: str, queue_conf: RocketMQQueueConfig):
        return cls(
            name_server=name_server,
            topic=queue_conf.topic,
            group=queue_conf.group,
            tags=queue_conf.tags,
            keys=queue_conf.keys,
            arguments=queue_conf.arguments,
        )

    def __len__(self):
        """Return the number of messages currently buffered locally."""
        return self._msg_buffer.qsize()

    def pop(self, scheduler):
        """Pop a request from the local buffer (non-blocking)."""
        try:
            body = self._msg_buffer.get_nowait()
        except Empty:
            return None
        return self._make_request(body, scheduler)

    def _make_request(self, body, scheduler):
        if isinstance(body, bytes):
            body = body.decode()
        return self._request_from_dict(json.loads(body), scheduler.spider)

    @_try_operation
    def push(self, request, scheduler):
        """Push a request onto the RocketMQ topic."""
        body: str = json.dumps(
            keys_string(self._request_to_dict(request, scheduler.spider)),
            cls=BytesDump,
        )
        logger.debug(f"push message, body: {body}")
        msg = Message(self.topic)
        if self.tags:
            msg.set_tags(self.tags)
        if self.keys:
            msg.set_keys(self.keys)
        msg.set_body(body.encode())
        self.producer.send_sync(msg)

    def _on_message(self, msg):
        """Callback invoked by PushConsumer for each received message."""
        try:
            self._msg_buffer.put(msg.body)
            return ConsumeStatus.CONSUME_SUCCESS
        except Exception:
            logger.exception("Error buffering RocketMQ message")
            return ConsumeStatus.RECONSUME_LATER

    def connect(self):
        """Establish producer and consumer connections."""
        logger.info(f"connect RocketMQ: {self.name_server}")
        if self.producer:
            try:
                self.producer.shutdown()
            except Exception:
                pass
        self.producer = Producer(self.group)
        self.producer.set_name_server_address(self.name_server)
        self.producer.start()

        consumer_group = f"{self.group}.spider.consumer"
        if self.consumer:
            try:
                self.consumer.shutdown()
            except Exception:
                pass
        self.consumer = PushConsumer(consumer_group)
        self.consumer.set_name_server_address(self.name_server)
        self.consumer.subscribe(self.topic, self._on_message)
        self.consumer.start()

    def close(self):
        """Shut down producer and consumer."""
        logger.info("close RocketMQ connection")
        if self.producer:
            try:
                self.producer.shutdown()
            except Exception:
                pass
        if self.consumer:
            try:
                self.consumer.shutdown()
            except Exception:
                pass

    def clear(self):
        """Discard all locally buffered messages."""
        while not self._msg_buffer.empty():
            try:
                self._msg_buffer.get_nowait()
            except Empty:
                break

    @classmethod
    def _request_from_dict(cls, d, spider=None):
        """Reconstruct a :class:`scrapy.http.Request` from a serialised dict."""
        return request_from_dict(d, spider=spider)

    @classmethod
    def _request_to_dict(cls, request, spider=None):
        d = request.to_dict(spider=spider)
        new_dict = {}
        for key, value in d.items():
            if value:
                new_dict[key] = value
        logger.debug(f"request_to_dict: {d}")
        return new_dict


__all__ = ["RocketMQQueue"]
