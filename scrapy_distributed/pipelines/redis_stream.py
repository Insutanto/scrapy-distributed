#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

from scrapy_distributed.common.queue_config import RedisStreamQueueConfig
from scrapy_distributed.redis_utils.connection import get_redis_from_settings


default_serialize = ScrapyJSONEncoder().encode

logger = logging.getLogger(__name__)


class RedisStreamPipeline(object):
    def __init__(self, item_conf: RedisStreamQueueConfig, redis_client):
        self.item_conf = item_conf
        self.redis_client = redis_client
        self.serialize = default_serialize

    @classmethod
    def from_crawler(cls, crawler):
        if hasattr(crawler.spider, "item_conf"):
            item_conf = crawler.spider.item_conf
        else:
            item_conf = RedisStreamQueueConfig(cls.item_key(None, crawler.spider))
        return cls(
            item_conf=item_conf,
            redis_client=get_redis_from_settings(crawler.settings),
        )

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        payload = item._values if hasattr(item, "_values") else dict(item)
        data = self.serialize(payload)
        kwargs = dict(self.item_conf.arguments or {})
        self.redis_client.xadd(
            name=self.item_conf.name,
            fields={"data": data},
            id=self.item_conf.id,
            maxlen=self.item_conf.maxlen,
            approximate=self.item_conf.approximate,
            **kwargs,
        )
        spider.logger.info(f"produce: {data}")
        return item

    @classmethod
    def item_key(cls, item, spider):
        return f"{spider.name}:items"

    def close(self):
        close = getattr(self.redis_client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
        logger.error("redis stream pipeline client is closed")
