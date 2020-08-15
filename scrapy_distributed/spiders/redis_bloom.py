#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib.parse import urlparse

from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig
from scrapy_distributed.redis_utils import defaults
from scrapy_distributed.redis_utils.connection import get_redis_from_settings


class RedisBloomMixin(object):
    redis_bloom_conf: RedisBloomConfig

    def __init__(self, redis_client=None, bloom_key=None):
        self.redis_client = redis_client
        self.bloom_key = bloom_key

    def setup_redis_client(self, crawler):
        settings = crawler.settings
        self.redis_client = get_redis_from_settings(settings)
        if hasattr(self, "redis_bloom_conf"):
            self.bloom_key = self.redis_bloom_conf.key
        else:
            dupe_filter_key = settings.get(
                "REDIS_BLOOM_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY
            )
            self.bloom_key = dupe_filter_key % {"spider": self.name}

    def request_success(self, url):
        result = urlparse(url)
        uri = result.netloc + result.path
        return self.redis_client.bfAdd(self.bloom_key, uri)

