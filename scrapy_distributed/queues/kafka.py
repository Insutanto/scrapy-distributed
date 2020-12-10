#!/usr/bin/env python
# -*- coding: utf-8 -*-
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from scrapy.http.request import Request
from scrapy.utils.misc import load_object

from scrapy.utils.reqser import _get_method, request_to_dict
from w3lib.util import to_unicode
from scrapy_distributed.queues.common import BytesDump, keys_string

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka import KafkaConsumer
from scrapy_distributed.queues import IQueue
from scrapy_distributed.common.queue_config import KafkaQueueConfig
import time


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


class KafkaQueue(IQueue):
    """Per-spider FIFO queue"""

    def __init__(
        self,
        connection_conf,
        name,
        num_partitions = 10,
        replication_factor = 1,
        arguments=None,
    ):
        """Initialize per-spider Kafka queue.

        Parameters:
            connection_conf -- Kafka connection_conf
            key -- rabbitmq routing key
        """
        self.connection_conf = connection_conf
        self.name = name
        self.topic = f"{name}.spider.queue"
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.arguments = arguments
        self.admin_client = None
        self.producer = None
        self.consumer = None
        self.connect()

    @classmethod
    def from_queue_conf(cls, connection_conf, queue_conf: KafkaQueueConfig):
        return cls(
            connection_conf,
            name=queue_conf.topic,
            num_partitions = queue_conf.num_partitions,
            replication_factor = queue_conf.replication_factor,
            arguments=queue_conf.arguments,
        )

    def __len__(self):
        """Return the length of the queue"""
        return 1

    @_try_operation
    def pop(self, scheduler):
        """Pop a message"""
        body = None
        try:
            poll_msg = self.consumer.poll(timeout_ms=500, update_offsets=True, max_records=1)
            for msgs in poll_msg.values():
                for msg in msgs:
                    body = msg.value
                    break
                break
        except Exception as e:
            logger.exception("kafaka queue, pop exception")
            return None
        if not body:
            return None
        request = self._make_request(body, scheduler)
        return request

    def _make_request(self, body, scheduler):
        return self._request_from_dict(
            json.loads(body.decode()), scheduler.spider 
        )

    @_try_operation
    def push(self, request, scheduler, headers=None, partition=0):
        """Push a message"""
        body: str = json.dumps(
                keys_string(self._request_to_dict(request, scheduler.spider)),
                cls=BytesDump,
            )
        logger.debug(f"push message, body: {body}")
        self.producer.send(self.topic, body.encode(), partition=partition)


    def connect(self):
        """Make a connection"""
        logger.info(f"connect kafka: {self.connection_conf}")
        if self.admin_client:
            try:
                self.admin_client.close()
            except:
                pass
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.connection_conf)
        topic_list = []
        topic_list.append(
            NewTopic(name=self.topic, 
                    num_partitions=self.num_partitions, 
                    replication_factor=self.replication_factor))
        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as e:
            logger.error(e)

        if self.producer:
            self.producer.close()
        self.producer = KafkaProducer(bootstrap_servers=self.connection_conf)
        if self.consumer:
            self.consumer.close()
        self.consumer = KafkaConsumer(self.topic, group_id=f"{self.topic}.spider.consumer", bootstrap_servers=self.connection_conf)

    def close(self):
        """Close channel"""
        logger.error(f"close Kafka connection")
        self.admin_client.close()
        self.producer.close()
        self.consumer.close()

    def clear(self):
        """Clear queue/stack"""
        # self.channel.queue_purge(self.name)

    @classmethod
    def _request_from_dict(cls, d, spider=None):
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
