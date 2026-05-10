#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import random

logger = logging.getLogger(__name__)

try:
    from scrapy_playwright.page import PageMethod
except ImportError:  # pragma: no cover
    PageMethod = None


class DynamicCrawlerMiddleware:
    """
    Optional middleware for dynamic pages and basic anti-crawling hardening.
    """

    def __init__(self, settings):
        self.user_agents = settings.get("DYNAMIC_CRAWLER_USER_AGENTS", [])
        self.proxies = settings.get("DYNAMIC_CRAWLER_PROXIES", [])
        self.block_statuses = set(settings.get("DYNAMIC_CRAWLER_BLOCK_STATUSES", [403, 429]))
        self.max_retry_times = settings.get("DYNAMIC_CRAWLER_MAX_RETRY_TIMES", 2)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        if self.user_agents and "User-Agent" not in request.headers:
            request.headers["User-Agent"] = random.choice(self.user_agents)

        if self.proxies and "proxy" not in request.meta:
            request.meta["proxy"] = random.choice(self.proxies)

        click_selectors = request.meta.get("dynamic_click_selectors", [])
        if click_selectors:
            request.meta["playwright"] = True
            if PageMethod is None:
                logger.warning("dynamic_click_selectors set but scrapy-playwright is not installed.")
                return None

            methods = request.meta.setdefault("playwright_page_methods", [])
            for selector in click_selectors:
                methods.append(PageMethod("click", selector))
        return None

    def process_response(self, request, response, spider):
        if response.status in self.block_statuses:
            retry_times = request.meta.get("dynamic_retry_times", 0)
            if retry_times < self.max_retry_times:
                retry_request = request.copy()
                retry_request.dont_filter = True
                retry_request.meta["dynamic_retry_times"] = retry_times + 1
                spider.logger.info("Retry blocked response %s for %s", response.status, request.url)
                return retry_request
        return response
