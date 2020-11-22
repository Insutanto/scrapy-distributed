#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import Request
from scrapy_distributed.middlewares.common import is_a_picture
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware


class RabbitMiddleware(RedirectMiddleware):
    """ Middleware used to close message from current queue or
        send unsuccessful messages to be rescheduled.
    """

    def __init__(self, settings):
        super().__init__(settings)
        self.requeue_list = settings.get("SCHEDULER_REQUEUE_ON_STATUS", [])
        self.init = True

    @classmethod
    def from_settings(cls, settings):
        return RabbitMiddleware(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return RabbitMiddleware(crawler.settings)

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
        spider.logger.debug(f"process_request: {spider.name}: {request.url}, {request.meta.get('delivery_tag', None)}")
        return None

    def process_response(self, request, response, spider):
        self.ensure_init(spider)
        spider.logger.debug(f"process_response: {spider.name}: {response.url}")
        redirect_result = super().process_response(request, response, spider)
        if isinstance(redirect_result, Request):
            self.ack(request, response)
            return redirect_result
        if not is_a_picture(response):
            if response.status in self.requeue_list:
                self.requeue(response)
                self.ack(request, response)
                request.meta["requeued"] = True
                raise IgnoreRequest
            else:
                self.ack(request, response)
        else:
            self.process_picture(response)
        return response

    def has_delivery_tag(self, request):
        if "delivery_tag" not in request.meta:
            self.spider.logger.debug("Request %(request)s does not have a deliver tag." % {"request": request})
            return False
        return True

    def process_picture(self, response):
        self.spider.logger.debug("Picture (%(status)d): %(url)s", {"url": response.url, "status": response.status})
        self.inc_stat("picture")

    def requeue(self, response):
        self.scheduler.requeue_message(response.url)
        self.spider.logger.debug("Requeued (%(status)d): %(url)s", {"url": response.url, "status": response.status})
        self.inc_stat("requeued")

    def ack(self, request, response):
        self.spider.logger.debug(f"ack Request({request}): {request.meta}")
        if self.has_delivery_tag(request):
            delivery_tag = request.meta.get("delivery_tag")
            self.scheduler.queue.ack(delivery_tag)
            self.spider.logger.debug("Acked (%(status)d): %(url)s" % {"url": response.url, "status": response.status})
            self.inc_stat("acked")
        else:
            self.spider.logger.info(f"don't has_delivery_tag: {request.url}")

    def inc_stat(self, stat):
        self.stats.inc_value("scheduler/acking/%(stat)s/distributed-queue" % {"stat": stat}, spider=self.spider)
