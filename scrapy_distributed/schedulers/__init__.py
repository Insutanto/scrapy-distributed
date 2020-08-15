#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/6/28 4:49 PM

from scrapy_distributed.schedulers.redis_bloom import RedisBloomSchedulerMixin
from scrapy_distributed.schedulers.amqp import RabbitScheduler
import logging

logger = logging.getLogger(__name__)


class DistributedScheduler(RabbitScheduler, RedisBloomSchedulerMixin):

    def open(self, spider):
        logger.info("DistributedScheduler, open")
        super(DistributedScheduler, self).open(spider)
        self.init_redis_bloom(spider)


__all__ = ["amqp", "redis_bloom", "DistributedScheduler"]