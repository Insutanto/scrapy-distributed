#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from scrapy.utils.misc import load_object

logger = logging.getLogger(__name__)


class DupeFilterSchedulerMixin(object):
    """Generic scheduler mixin that initialises any DupeFilter from a spider.

    Works with any DupeFilter that extends
    :class:`scrapy_distributed.dupefilters.base.BaseDupeFilter` (or any class
    that exposes a ``from_spider(spider)`` classmethod).

    The ``DUPEFILTER_CLASS`` Scrapy setting is used to locate the filter
    class; ``from_spider`` is then called so that the filter can read
    spider-level configuration (e.g. a per-spider key or capacity).

    Usage::

        class MyScheduler(DistributedQueueScheduler, DupeFilterSchedulerMixin):
            def open(self, spider):
                super().open(spider)
                self.init_dupefilter(spider)
    """

    def init_dupefilter(self, spider):
        """Initialise ``self.df`` from the spider using ``from_spider``.

        Parameters
        ----------
        spider : scrapy.Spider
        """
        logger.debug("DupeFilterSchedulerMixin, init_dupefilter")
        dupefilter_class_path = spider.settings.get("DUPEFILTER_CLASS")
        if not dupefilter_class_path:
            raise KeyError(
                "DUPEFILTER_CLASS is not set. "
                "Add 'DUPEFILTER_CLASS = \"<your.DupeFilter>\"' to your Scrapy settings."
            )
        dupefilter_cls = load_object(dupefilter_class_path)
        self.df = dupefilter_cls.from_spider(spider)
