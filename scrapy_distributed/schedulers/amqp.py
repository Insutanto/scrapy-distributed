#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import time

from scrapy import Request
from scrapy.core.scheduler import Scheduler
from scrapy.utils.job import job_dir
from scrapy.utils.misc import create_instance, load_object
from scrapy.utils.python import to_unicode
from scrapy.utils.reqser import _get_method, request_to_dict
from queuelib import PriorityQueue
import warnings
from scrapy_distributed.queues.amqp import QueueConfig
from scrapy.utils.deprecate import ScrapyDeprecationWarning

logger = logging.getLogger(__name__)


class BytesDump(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode()
        return json.JSONEncoder.default(self, obj)


def keys_string(d):
    rval = {}
    if not isinstance(d, dict):
        if isinstance(d, (tuple, list, set)):
            v = [keys_string(x) for x in d]
            return v
        else:
            return d

    for k, v in d.items():
        if isinstance(k, bytes):
            k = k.decode()
        if isinstance(v, dict):
            v = keys_string(v)
        elif isinstance(v, (tuple, list, set)):
            v = [keys_string(x) for x in v]
        rval[k] = v
    return rval


class RabbitScheduler(Scheduler):
    def __init__(
        self,
        dupe_filter,
        connection_url=None,
        queue_class=None,
        logunser=False,
        stats=None,
        pqclass=None,
        dqclass=None,
        mqclass=None,
        jobdir=None,
        crawler=None,
    ):
        super(RabbitScheduler, self).__init__(
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
        self.connection_url = connection_url
        self.waiting = False
        self.closing = False
        self.queue = None
        self.spider = None
        self.mqs = None
        self.dqs = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        connection_url = settings.get("RABBITMQ_CONNECTION_PARAMETERS")
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
        return cls(
            dupefilter,
            connection_url,
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
        logger.info("RabbitScheduler, open")
        self.spider = spider
        if not hasattr(spider, "name"):
            msg = "Please set name parameter to spider. "
            raise ValueError(msg)

        if not hasattr(spider, "queue_conf"):
            queue_conf: QueueConfig = QueueConfig(spider.name)
        else:
            queue_conf = spider.queue_conf

        self.queue = self.queue_class.from_queue_conf(self.connection_url, queue_conf)

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
        enqueue_ok = self._amqp_push(request)
        if enqueue_ok:
            self.stats.inc_value("scheduler/enqueued/amqp", spider=self.spider)
            logger.info(f"AMQP push request, url: {request.url}")
            return True
        else:
            return super().enqueue_request(request)

    def next_request(self):
        if self.closing:
            return
        if super().__len__() > 0:
            return super().next_request()

        request = self._amqp_pop()
        if request:
            self.waiting = False
            if self.stats:
                self.stats.inc_value("scheduler/dequeued/amqp", spider=self.spider)
            return request
        else:
            if not self.waiting:
                msg = "Queue {} is empty. Waiting for messages..."
                self.waiting = True
                logger.error(msg.format(self.queue.name))
            time.sleep(1)
            return None

    def has_pending_requests(self):
        return not self.closing

    def requeue_message(self, body, headers=None):
        self.queue.push(body, headers)

    def _amqp_push(self, request):
        if self.queue is not None:
            self.queue.push(
                json.dumps(
                    keys_string(self._request_to_dict(request, self.spider)),
                    cls=BytesDump,
                )
            )
            return True
        return False

    def _amqp_pop(self):
        method, properties, body = self.queue.pop()
        if body is None:
            return None
        request = self._make_request(method, properties, body)
        request.meta["delivery_tag"] = method.delivery_tag
        return request

    def _make_request(self, method, properties, body):
        return self._request_from_dict(
            json.loads(body.decode()), self.spider, method, properties
        )

    def ack_message(self, delivery_tag):
        self.queue.ack(delivery_tag)

    @classmethod
    def _request_from_dict(cls, d, spider=None, method=None, properties=None):
        """Create Request object from a dict.

        If a spider is given, it will try to resolve the callbacks looking at the
        spider for methods with the same name.
        """
        cb = d.get("callback", None)
        if cb and spider:
            cb = _get_method(spider, cb)
        eb = d.get("errback", None)
        if eb and spider:
            eb = _get_method(spider, eb)
        request_cls = load_object(d.get("_class", None)) if "_class" in d else Request
        return request_cls(
            url=to_unicode(d.get("url", None)),
            callback=cb,
            errback=eb,
            method=d.get("method", None),
            headers=d.get("headers", None),
            body=d.get("body", None),
            cookies=d.get("cookies", None),
            meta=d.get("meta", None),
            encoding=d.get("_encoding", None),
            priority=d.get("priority", 0),
            dont_filter=d.get("dont_filter", True),
            flags=d.get("flags", None),
        )

    @classmethod
    def _request_to_dict(cls, request, spider=None):
        d = request_to_dict(request, spider)
        new_dict = dict()
        for key, value in d.items():
            if value:
                new_dict[key] = value
        logger.debug(f"request_to_dict: {d}")
        return new_dict
