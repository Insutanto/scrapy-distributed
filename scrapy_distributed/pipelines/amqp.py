#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from scrapy_distributed.common.queue_config import RabbitQueueConfig
from scrapy.utils.serialize import ScrapyJSONEncoder
from scrapy_distributed.amqp_utils import connection
from twisted.internet.threads import deferToThread

default_serialize = ScrapyJSONEncoder().encode

logger = logging.getLogger(__name__)


class RabbitPipeline(object):
    def __init__(self, item_conf: RabbitQueueConfig, connection_url: str):
        self.item_conf = item_conf
        self.serialize = default_serialize
        self.connection_url = connection_url
        self.connection = None
        self.channel = None
        self.connect()
    
    @classmethod
    def from_crawler(cls, crawler):
        if hasattr(crawler.spider, "item_conf"):
            item_conf = crawler.spider.item_conf
        else:
            item_conf = RabbitQueueConfig(cls.item_key(None, crawler.spider))
        return cls(
            item_conf=item_conf,
            connection_url=crawler.settings.get("RABBITMQ_CONNECTION_PARAMETERS"),
        )

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        data = self.serialize(item)
        self.channel.basic_publish(exchange="", routing_key=self.item_conf.name, body=data)
        return item

    @classmethod
    def item_key(cls, item, spider):
        return f"{spider.name}:items"

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
            queue=self.item_conf.name,
            passive=self.item_conf.passive,
            durable=self.item_conf.durable,
            exclusive=self.item_conf.exclusive,
            auto_delete=self.item_conf.auto_delete,
            arguments=self.item_conf.arguments,
        )

    def close(self):
        """Close channel"""
        self.channel.close()
        logger.error("rabbitmq pipeline channel is closed")
