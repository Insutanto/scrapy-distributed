#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from rocketmq.client import Producer, Message
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

from scrapy_distributed.common.queue_config import RocketMQQueueConfig


default_serialize = ScrapyJSONEncoder().encode

logger = logging.getLogger(__name__)


class RocketMQPipeline(object):
    def __init__(self, item_conf: RocketMQQueueConfig, name_server: str):
        self.item_conf = item_conf
        self.name_server = name_server
        self.serialize = default_serialize
        self.producer = None
        self.connect()

    @classmethod
    def from_crawler(cls, crawler):
        if hasattr(crawler.spider, "item_conf"):
            item_conf = crawler.spider.item_conf
        else:
            item_conf = RocketMQQueueConfig(cls.item_key(None, crawler.spider))
        return cls(
            item_conf=item_conf,
            name_server=crawler.settings.get("ROCKETMQ_NAME_SERVER"),
        )

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        body = self.serialize(item._values)
        msg = Message(self.item_conf.topic)
        if self.item_conf.tags:
            msg.set_tags(self.item_conf.tags)
        if self.item_conf.keys:
            msg.set_keys(self.item_conf.keys)
        msg.set_body(body.encode())
        ret = self.producer.send_sync(msg)
        spider.logger.info(f"produce: {body}, status: {ret.status}")
        return item

    @classmethod
    def item_key(cls, item, spider):
        return f"{spider.name}.items"

    def connect(self):
        logger.info(f"connect rocketmq: {self.name_server}")
        if self.producer:
            try:
                self.producer.shutdown()
            except Exception:
                pass
        self.producer = Producer(self.item_conf.group)
        self.producer.set_name_server_address(self.name_server)
        self.producer.start()

    def close(self):
        if self.producer:
            try:
                self.producer.shutdown()
            except Exception:
                pass
        logger.error("rocketmq pipeline producer is closed")
