#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import time

from scrapy.utils.request import request_from_dict

from scrapy_distributed.common.queue_config import RedisStreamQueueConfig
from scrapy_distributed.queues import IQueue
from scrapy_distributed.queues.common import BytesDump, keys_string
from scrapy_distributed.redis_utils.connection import get_redis


logger = logging.getLogger(__name__)


def _try_operation(function):
    """Wrap unary method by reconnect procedure"""

    def wrapper(self, *args, **kwargs):
        retries = 0
        last_exception = None
        while retries < 10:
            try:
                return function(self, *args, **kwargs)
            except Exception as e:
                retries += 1
                last_exception = e
                msg = "Function %s failed. Reconnecting... (%d times)" % (
                    str(function),
                    retries,
                )
                logger.error(msg)
                self.connect()
                time.sleep((retries - 1) * 5)
        if last_exception:
            raise last_exception
        return None

    return wrapper


class RedisStreamQueue(IQueue):
    """Per-spider Redis Stream queue."""

    def __init__(
        self,
        connection_conf,
        name,
        id="*",
        maxlen=None,
        approximate=True,
        arguments=None,
    ):
        self.connection_conf = connection_conf
        self.name = name
        self.id = id
        self.maxlen = maxlen
        self.approximate = approximate
        self.arguments = arguments
        self.redis_client = None
        self.last_id = "0-0"
        self.connect()

    @classmethod
    def from_queue_conf(cls, connection_conf, queue_conf: RedisStreamQueueConfig):
        return cls(
            connection_conf=connection_conf,
            name=queue_conf.name,
            id=queue_conf.id,
            maxlen=queue_conf.maxlen,
            approximate=queue_conf.approximate,
            arguments=queue_conf.arguments,
        )

    def __len__(self):
        return self.redis_client.xlen(self.name)

    @_try_operation
    def pop(self, scheduler):
        stream_items = self.redis_client.xread({self.name: self.last_id}, count=1)
        if not stream_items:
            return None
        _stream_name, messages = stream_items[0]
        if not messages:
            return None
        message_id, fields = messages[0]
        request = self._make_request(fields, scheduler)
        if request is None:
            return None
        request.meta["redis_stream_message_id"] = message_id
        self.last_id = message_id
        self.redis_client.xdel(self.name, message_id)
        return request

    def _make_request(self, fields, scheduler):
        decoded_fields = keys_string(fields)
        body = decoded_fields.get("data")
        if body is None:
            return None
        if isinstance(body, bytes):
            body = body.decode()
        return self._request_from_dict(json.loads(body), scheduler.spider)

    @_try_operation
    def push(self, request, scheduler, headers=None):
        body: str = json.dumps(
            keys_string(self._request_to_dict(request, scheduler.spider)),
            cls=BytesDump,
        )
        kwargs = dict(self.arguments or {})
        self.redis_client.xadd(
            name=self.name,
            fields={"data": body},
            id=self.id,
            maxlen=self.maxlen,
            approximate=self.approximate,
            **kwargs,
        )

    def connect(self):
        logger.info(f"connect redis stream: {self.connection_conf}")
        close = getattr(self.redis_client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
        if isinstance(self.connection_conf, str):
            self.redis_client = get_redis(url=self.connection_conf)
        elif isinstance(self.connection_conf, dict):
            self.redis_client = get_redis(**self.connection_conf)
        else:
            self.redis_client = get_redis()

    def close(self):
        close = getattr(self.redis_client, "close", None)
        if callable(close):
            close()

    def clear(self):
        self.redis_client.delete(self.name)

    @classmethod
    def _request_from_dict(cls, d, spider=None):
        """Create Request object from a dict."""
        return request_from_dict(d, spider=spider)

    @classmethod
    def _request_to_dict(cls, request, spider=None):
        d = request.to_dict(spider=spider)
        new_dict = dict()
        for key, value in d.items():
            if value:
                new_dict[key] = value
        logger.debug(f"request_to_dict: {d}")
        return new_dict


__all__ = ["RedisStreamQueue"]
