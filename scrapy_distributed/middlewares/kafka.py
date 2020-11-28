#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from scrapy.exceptions import IgnoreRequest
from scrapy_distributed.middlewares.common import is_a_picture

logger = logging.getLogger(__name__)


class KafkaMiddleware(object):
    """ Middleware used to close message from current queue or
        send unsuccessful messages to be rescheduled.
    """

    def __init__(self, settings):
        self.requeue_list = settings.get("SCHEDULER_REQUEUE_ON_STATUS", [])
        self.init = True

    @classmethod
    def from_settings(cls, settings):
        return KafkaMiddleware(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return KafkaMiddleware(crawler.settings)

    def ensure_init(self, spider):
        if self.init:
            self.spider = spider
            self.scheduler = spider.crawler.engine.slot.scheduler
            self.stats = spider.crawler.stats
            self.init = False

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        spider.logger.debug(f"process_request: {spider.name}: {request.url}")
        return None

    def process_response(self, request, response, spider):
        spider.logger.debug(f"process_response: {spider.name}: {response.url}")
        self.ensure_init(spider)
        if not is_a_picture(response):
            if response.status in self.requeue_list:
                self.requeue(response)
                request.meta["requeued"] = True
                raise IgnoreRequest
        else:
            self.process_picture(response)
        return response

    def process_picture(self, response):
        self.spider.debug("Picture (%(status)d): %(url)s", {"url": response.url, "status": response.status})
        self.inc_stat("picture")

    def requeue(self, response):
        self.scheduler.requeue_message(response.url)
        self.spider.debug("Requeued (%(status)d): %(url)s", {"url": response.url, "status": response.status})
        self.inc_stat("requeued")

    def inc_stat(self, stat):
        self.stats.inc_value("scheduler/acking/%(stat)s/distributed-queue" % {"stat": stat}, spider=self.spider)


