#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

from scrapy.core.scheduler import Scheduler
from scrapy.utils.job import job_dir
from scrapy.utils.misc import create_instance, load_object
from queuelib import PriorityQueue
import warnings
from scrapy_distributed.queues.kafka import KafkaQueue
from scrapy_distributed.queues.amqp import RabbitQueue
from scrapy_distributed.common.queue_config import RabbitQueueConfig, KafkaQueueConfig
from scrapy.utils.deprecate import ScrapyDeprecationWarning

logger = logging.getLogger(__name__)


class DistributedQueueScheduler(Scheduler):
    def __init__(
        self,
        dupe_filter,
        connection_conf=None,
        queue_class=None,
        logunser=False,
        stats=None,
        pqclass=None,
        dqclass=None,
        mqclass=None,
        jobdir=None,
        crawler=None,
    ):
        super(DistributedQueueScheduler, self).__init__(
            dupefilter=dupe_filter,
            jobdir=jobdir,
            logunser=logunser,
            stats=stats,
            pqclass=pqclass,
            dqclass=dqclass,
            mqclass=mqclass,
            crawler=crawler,
        )
        self.queue_class = queue_class
        self.connection_conf = connection_conf
        self.waiting = False
        self.closing = False
        self.queue = None
        self.spider = None
        self.mqs = None
        self.dqs = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        rabbit_connection_conf = settings.get("RABBITMQ_CONNECTION_PARAMETERS")
        kafka_connection_conf = settings.get("KAFKA_CONNECTION_PARAMETERS")
        custom_connection_conf = settings.get("CUSTOM_CONNECTION_PARAMETERS")
        queue_class = load_object(settings.get("SCHEDULER_QUEUE_CLASS"))
        dupefilter_cls = load_object(settings["DUPEFILTER_CLASS"])
        dupefilter = create_instance(dupefilter_cls, settings, crawler)
        pqclass = load_object(settings["SCHEDULER_PRIORITY_QUEUE"])
        if pqclass is PriorityQueue:
            warnings.warn(
                "SCHEDULER_PRIORITY_QUEUE='queuelib.PriorityQueue'"
                " is no longer supported because of API changes; "
                "please use 'scrapy.pqueues.ScrapyPriorityQueue'",
                ScrapyDeprecationWarning,
            )
            from scrapy.pqueues import ScrapyPriorityQueue

            pqclass = ScrapyPriorityQueue

        dqclass = load_object(settings["SCHEDULER_DISK_QUEUE"])
        mqclass = load_object(settings["SCHEDULER_MEMORY_QUEUE"])
        logunser = settings.getbool("SCHEDULER_DEBUG")
        if queue_class == RabbitQueue:
            connection_conf = rabbit_connection_conf
        elif queue_class == KafkaQueue:
            connection_conf = kafka_connection_conf
        else:
            connection_conf = custom_connection_conf
        logger.debug(f"connection_conf: {connection_conf}")

        return cls(
            dupefilter,
            connection_conf,
            jobdir=job_dir(settings),
            logunser=logunser,
            stats=crawler.stats,
            pqclass=pqclass,
            dqclass=dqclass,
            mqclass=mqclass,
            crawler=crawler,
            queue_class=queue_class,
        )

    def open(self, spider):
        logger.info("DistributedQueueScheduler, open")
        self.spider = spider
        if not hasattr(spider, "name"):
            msg = "Please set name parameter to spider. "
            raise ValueError(msg)

        if not hasattr(spider, "queue_conf"):
            if self.queue_class == RabbitQueue:
                queue_conf: RabbitQueueConfig = RabbitQueueConfig(spider.name)
            elif self.queue_class == KafkaQueue:
                queue_conf: KafkaQueueConfig = KafkaQueueConfig(spider.name)
            else:
                raise ValueError("Spider's queue hasn't been set.")
        else:
            queue_conf = spider.queue_conf
        
        self.queue = self.queue_class.from_queue_conf(self.connection_conf, queue_conf)

        msg_count = len(self.queue)
        if msg_count:
            logger.info("Resuming crawling ({} urls scheduled)".format(msg_count))
        else:
            logger.info("No items to crawl in {}".format(self.queue.name))

        self.spider = spider
        self.mqs = self._mq()
        self.dqs = self._dq() if self.dqdir else None
        self.df.open()
        return

    def close(self, reason):
        return self.df.close(reason)

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            logger.debug(f"filter, request: {request.url}")
            self.df.log(request, self.spider)
            return False
        enqueue_ok = self.push_distributed_queue(request)
        if enqueue_ok:
            self.stats.inc_value("scheduler/enqueued/distributed-queue", spider=self.spider)
            return True
        else:
            return super().enqueue_request(request)

    def next_request(self):
        if self.closing:
            return
        if super().__len__() > 0:
            return super().next_request()

        request = self.pop_distributed_queue()
        if request:
            self.waiting = False
            if self.stats:
                self.stats.inc_value("scheduler/dequeued/distributed-queue", spider=self.spider)
            return request
        else:
            if not self.waiting:
                msg = "Queue {} is empty. Waiting for messages..."
                self.waiting = True
                logger.info(msg.format(self.queue.name))
            time.sleep(1)
            return None

    def has_pending_requests(self):
        return not self.closing

    def requeue_message(self, body, headers=None):
        self.queue.push(body, headers)

    def push_distributed_queue(self, request):
        if self.queue is not None:
            self.queue.push(request, scheduler=self)
            return True
        return False

    def pop_distributed_queue(self):
        return self.queue.pop(scheduler=self)
