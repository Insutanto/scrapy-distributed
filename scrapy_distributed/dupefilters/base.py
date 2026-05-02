#!/usr/bin/env python
# -*- coding: utf-8 -*-
from scrapy.dupefilters import BaseDupeFilter as ScrapyBaseDupeFilter


class BaseDupeFilter(ScrapyBaseDupeFilter):
    """Base class for scrapy-distributed DupeFilters.

    Extends Scrapy's BaseDupeFilter with a ``from_spider`` classmethod that
    allows the scheduler to initialise the filter with spider-level
    configuration (e.g. a per-spider bloom-filter key or capacity).

    Custom DupeFilter implementations should extend this class and override
    ``from_spider`` (and, if needed, ``request_seen``, ``open``, and
    ``close``).

    Example::

        class MyDupeFilter(BaseDupeFilter):
            @classmethod
            def from_spider(cls, spider):
                instance = cls.from_settings(spider.settings)
                # apply spider-specific config here
                return instance

            def request_seen(self, request):
                ...
    """

    @classmethod
    def from_spider(cls, spider):
        """Create an instance initialised with spider-level configuration.

        The default implementation simply delegates to ``from_settings``.
        Subclasses should override this method to read spider attributes
        (e.g. ``spider.dupefilter_conf``) and apply them to the instance
        before returning it.

        Parameters
        ----------
        spider : scrapy.Spider

        Returns
        -------
        BaseDupeFilter
        """
        settings = spider.settings
        return cls.from_settings(settings)
