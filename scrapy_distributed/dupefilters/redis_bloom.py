#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from logging import log
from os import dup
from urllib.parse import urlparse

from redisbloom.client import Client
from scrapy.dupefilters import BaseDupeFilter

from scrapy_distributed.redis_utils import defaults
from scrapy_distributed.redis_utils.connection import get_redis_from_settings

logger = logging.getLogger(__name__)


class RedisBloomConfig(object):
    def __init__(self, key, error_rate=0.001, capacity=100_0000, kwargs=None):
        self.key = key
        self.error_rate = error_rate
        self.capacity = capacity
        self.kwargs = kwargs if kwargs is not None else {}


class RedisBloomDupeFilter(BaseDupeFilter):
    def __init__(
        self,
        redis_client: Client,
        config: RedisBloomConfig = None,
        dupe_filter_key: str = defaults.SCHEDULER_DUPEFILTER_KEY,
        default_error_rate: float = defaults.DUPEFILTER_ERROR_RATE,
        default_capacity: int = defaults.DUPEFILTER_CAPACITY,
        debug: bool = False,
    ):
        self.file = None
        self.fingerprints = set()
        self.redis_client = redis_client
        self.dupe_filter_key = dupe_filter_key
        self.default_error_rate = default_error_rate
        self.default_capacity = default_capacity
        self.logdupes = True
        self.debug = debug
        self.config = config
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_settings(cls, settings):
        logger.debug("RedisBloomDupeFilter from_settings")
        debug = settings.getbool("DUPEFILTER_DEBUG")
        dupe_filter_key = settings.get(
            "SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY
        )
        default_error_rate = settings.get(
            "BLOOM_DUPEFILTER_ERROR_RATE", defaults.DUPEFILTER_ERROR_RATE
        )
        default_capacity = settings.get(
            "BLOOM_DUPEFILTER_CAPACITY", defaults.DUPEFILTER_CAPACITY
        )
        redis_client = get_redis_from_settings(settings)
        return cls(
            redis_client,
            dupe_filter_key=dupe_filter_key,
            default_error_rate=default_error_rate,
            default_capacity=default_capacity,
            debug=debug,
        )

    @classmethod
    def from_crawler(cls, crawler):
        """Returns instance from crawler.

        Parameters
        ----------
        crawler : scrapy.crawler.Crawler

        Returns
        -------
        RFPDupeFilter
            Instance of RFPDupeFilter.

        """
        logger.debug("RedisBloomDupeFilter from_crawler")
        return cls.from_settings(crawler.settings)

    @classmethod
    def from_spider(cls, spider):
        logger.debug("RedisBloomDupeFilter from_spider")
        settings = spider.settings
        redis_client = get_redis_from_settings(settings)
        instance = cls.from_settings(settings)
        instance.init_redis_bloom_key(spider)
        return instance

    def request_seen(self, request):
        """Returns True if request was already seen.

        Parameters
        ----------
        request : scrapy.http.Request

        Returns
        -------
        bool

        """
        result = urlparse(request.url)
        uri = result.netloc + result.path
        seen = self.redis_client.bfExists(self.config.key, uri) == 1
        if not seen:
            self.redis_client.bfAdd(self.config.key, uri)
        return seen

    def request_success(self, url):
        result = urlparse(url)
        uri = result.netloc + result.path
        return self.redis_client.bfAdd(self.config.key, uri)

    def init_redis_bloom_key(self, spider):
        if spider:
            if hasattr(spider, "redis_bloom_conf"):
                self.config: RedisBloomConfig = spider.redis_bloom_conf
        if self.config is None:
            self.config: RedisBloomConfig = RedisBloomConfig(
                self.dupe_filter_key % {"spider": spider.name},
                self.default_error_rate,
                self.default_capacity,
            )

        if self.config:
            if self.redis_client.exists(self.config.key) == 0:
                self.redis_client.bfCreate(
                    self.config.key, self.config.error_rate, self.config.capacity
                )

    def open(self, spider=None):
        if spider is None:
            return

        self.init_redis_bloom_key(spider)

    def close(self, reason=""):
        """Delete data on close. Called by Scrapy's scheduler.

        Parameters
        ----------
        reason : str, optional

        """
        pass

    def clear(self):
        """Clears fingerprints data."""
        self.redis_client.delete(self.config.key)
