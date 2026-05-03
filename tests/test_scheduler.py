"""
Unit tests for scrapy_distributed.schedulers.common.DistributedQueueScheduler.

All queue / broker interactions are mocked – no real broker required.
"""
import sys
import types
from types import SimpleNamespace
import pytest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out rocketmq before importing anything that pulls it in.
# ---------------------------------------------------------------------------
_rocketmq_stub = types.ModuleType("rocketmq")
_rocketmq_client_stub = types.ModuleType("rocketmq.client")
_rocketmq_client_stub.ConsumeStatus = SimpleNamespace(
    CONSUME_SUCCESS="OK", RECONSUME_LATER="LATER"
)
_rocketmq_client_stub.Producer = MagicMock
_rocketmq_client_stub.PushConsumer = MagicMock
_rocketmq_client_stub.Message = MagicMock
sys.modules.setdefault("rocketmq", _rocketmq_stub)
sys.modules.setdefault("rocketmq.client", _rocketmq_client_stub)

from scrapy_distributed.schedulers.common import DistributedQueueScheduler  # noqa: E402
from scrapy_distributed.queues.amqp import RabbitQueue  # noqa: E402
from scrapy_distributed.queues.kafka import KafkaQueue  # noqa: E402
from scrapy_distributed.queues.rocketmq import RocketMQQueue  # noqa: E402
from scrapy_distributed.common.queue_config import (  # noqa: E402
    RabbitQueueConfig,
    KafkaQueueConfig,
    RocketMQQueueConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dupefilter():
    df = MagicMock()
    df.request_seen.return_value = False
    return df


def _make_pqclass():
    from scrapy.pqueues import ScrapyPriorityQueue
    return ScrapyPriorityQueue


def _make_scheduler(queue_class=None, connection_conf=None):
    """Create a DistributedQueueScheduler with mocked dependencies."""
    df = _make_dupefilter()
    stats = MagicMock()
    crawler = MagicMock()
    crawler.stats = stats

    from scrapy.pqueues import ScrapyPriorityQueue
    from scrapy.squeues import PickleFifoDiskQueue, FifoMemoryQueue

    scheduler = DistributedQueueScheduler(
        dupe_filter=df,
        connection_conf=connection_conf or "amqp://localhost/",
        queue_class=queue_class or RabbitQueue,
        logunser=False,
        stats=stats,
        pqclass=ScrapyPriorityQueue,
        dqclass=PickleFifoDiskQueue,
        mqclass=FifoMemoryQueue,
        jobdir=None,
        crawler=crawler,
    )
    return scheduler


def _make_spider(name="myspider"):
    spider = MagicMock()
    spider.name = name
    spider.settings = MagicMock()
    return spider


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerInit:
    def test_initial_state(self):
        s = _make_scheduler()
        assert s.queue is None
        assert s.waiting is False
        assert s.closing is False
        assert s.spider is None


# ---------------------------------------------------------------------------
# from_crawler
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerFromCrawler:
    def _make_crawler(self, queue_class_path, extra=None):
        settings = {
            "SCHEDULER_QUEUE_CLASS": queue_class_path,
            "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
            "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.ScrapyPriorityQueue",
            "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
            "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
            "SCHEDULER_START_DISK_QUEUE": None,
            "SCHEDULER_START_MEMORY_QUEUE": None,
            "SCHEDULER_DEBUG": False,
            "RABBITMQ_CONNECTION_PARAMETERS": "amqp://localhost/",
            "KAFKA_CONNECTION_PARAMETERS": "localhost:9092",
            "ROCKETMQ_NAME_SERVER": "127.0.0.1:9876",
            "CUSTOM_CONNECTION_PARAMETERS": None,
            "JOBDIR": None,
        }
        if extra:
            settings.update(extra)

        crawler = MagicMock()
        crawler.settings.get = lambda k, default=None: settings.get(k, default)
        crawler.settings.getbool = lambda k, default=False: bool(settings.get(k, default))
        crawler.settings.__getitem__ = lambda self_inner, k: settings.get(k)
        crawler.stats = MagicMock()
        return crawler

    def test_from_crawler_rabbit_sets_rabbit_connection(self):
        crawler = self._make_crawler(
            "scrapy_distributed.queues.amqp.RabbitQueue"
        )
        with patch("scrapy_distributed.schedulers.common.build_from_crawler") as mock_bfc:
            mock_bfc.return_value = MagicMock()
            scheduler = DistributedQueueScheduler.from_crawler(crawler)
        assert scheduler.connection_conf == "amqp://localhost/"
        assert scheduler.queue_class is RabbitQueue

    def test_from_crawler_kafka_sets_kafka_connection(self):
        crawler = self._make_crawler(
            "scrapy_distributed.queues.kafka.KafkaQueue"
        )
        with patch("scrapy_distributed.schedulers.common.build_from_crawler") as mock_bfc:
            mock_bfc.return_value = MagicMock()
            scheduler = DistributedQueueScheduler.from_crawler(crawler)
        assert scheduler.connection_conf == "localhost:9092"
        assert scheduler.queue_class is KafkaQueue

    def test_from_crawler_rocketmq_sets_name_server(self):
        crawler = self._make_crawler(
            "scrapy_distributed.queues.rocketmq.RocketMQQueue"
        )
        with patch("scrapy_distributed.schedulers.common.build_from_crawler") as mock_bfc:
            mock_bfc.return_value = MagicMock()
            scheduler = DistributedQueueScheduler.from_crawler(crawler)
        assert scheduler.connection_conf == "127.0.0.1:9876"
        assert scheduler.queue_class is RocketMQQueue


# ---------------------------------------------------------------------------
# open
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerOpen:
    def _open_with_mock_queue(self, queue_class, spider_name="myspider",
                               has_queue_conf=False, msg_count=0,
                               connection_conf="amqp://localhost/"):
        mock_queue = MagicMock()
        mock_queue.__len__ = MagicMock(return_value=msg_count)
        mock_queue.name = "test-queue"

        spider = _make_spider(spider_name)
        if has_queue_conf:
            spider.queue_conf = RabbitQueueConfig(name=spider_name)

        s = _make_scheduler(queue_class=queue_class,
                             connection_conf=connection_conf)

        with patch.object(queue_class, "from_queue_conf", return_value=mock_queue):
            s.open(spider)

        return s, mock_queue

    def test_open_creates_rabbit_queue_from_spider_name(self):
        s, q = self._open_with_mock_queue(RabbitQueue)
        assert s.queue is q

    def test_open_creates_kafka_queue_from_spider_name(self):
        s, q = self._open_with_mock_queue(KafkaQueue)
        assert s.queue is q

    def test_open_creates_rocketmq_queue_from_spider_name(self):
        s, q = self._open_with_mock_queue(
            RocketMQQueue, connection_conf="127.0.0.1:9876"
        )
        assert s.queue is q

    def test_open_uses_spider_queue_conf_when_present(self):
        s, q = self._open_with_mock_queue(RabbitQueue, has_queue_conf=True)
        assert s.queue is q

    def test_open_raises_when_spider_has_no_name(self):
        s = _make_scheduler()
        spider = MagicMock(spec=[])  # no 'name' attribute
        with pytest.raises((ValueError, AttributeError)):
            s.open(spider)

    def test_open_raises_for_unknown_queue_class_without_conf(self):
        """Unknown queue_class + no queue_conf → ValueError."""
        class MyCustomQueue:
            @classmethod
            def from_queue_conf(cls, conf, queue_conf):
                return MagicMock()

        s = _make_scheduler(queue_class=MyCustomQueue)
        # Use SimpleNamespace so hasattr(spider, "queue_conf") is False
        spider = SimpleNamespace(name="testspider", settings=MagicMock())
        with pytest.raises(ValueError):
            s.open(spider)


# ---------------------------------------------------------------------------
# enqueue_request
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerEnqueueRequest:
    def _make_ready_scheduler(self, queue_class=RabbitQueue):
        s = _make_scheduler(queue_class=queue_class)
        s.queue = MagicMock()
        s.queue.name = "q"
        s.spider = _make_spider()
        s.stats = MagicMock()
        s.df = _make_dupefilter()
        return s

    def test_enqueue_pushes_to_distributed_queue(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        request = Request("http://example.com/", dont_filter=True)
        result = s.enqueue_request(request)
        assert result is True
        s.queue.push.assert_called_once()

    def test_enqueue_increments_stats(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        request = Request("http://example.com/", dont_filter=True)
        s.enqueue_request(request)
        s.stats.inc_value.assert_called()

    def test_enqueue_filters_seen_request(self):
        s = self._make_ready_scheduler()
        s.df.request_seen.return_value = True
        from scrapy.http import Request
        request = Request("http://example.com/", dont_filter=False)
        result = s.enqueue_request(request)
        assert result is False
        s.queue.push.assert_not_called()


# ---------------------------------------------------------------------------
# next_request
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerNextRequest:
    def _make_ready_scheduler(self):
        s = _make_scheduler()
        s.queue = MagicMock()
        s.queue.name = "q"
        s.spider = _make_spider()
        s.stats = MagicMock()
        s.df = _make_dupefilter()
        # Initialize mqs/dqs so super().__len__() works
        from scrapy.squeues import FifoMemoryQueue
        s.mqs = FifoMemoryQueue()
        s.dqs = None
        return s

    def test_next_request_returns_from_distributed_queue(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        mock_request = Request("http://example.com/")
        s.queue.pop.return_value = mock_request
        result = s.next_request()
        assert result is mock_request

    def test_next_request_returns_none_and_sets_waiting_when_empty(self):
        s = self._make_ready_scheduler()
        s.queue.pop.return_value = None
        with patch("scrapy_distributed.schedulers.common.time") as mock_time:
            result = s.next_request()
        assert result is None
        assert s.waiting is True

    def test_next_request_returns_none_when_closing(self):
        s = self._make_ready_scheduler()
        s.closing = True
        result = s.next_request()
        assert result is None

    def test_next_request_increments_stats_on_success(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        s.queue.pop.return_value = Request("http://example.com/")
        s.next_request()
        s.stats.inc_value.assert_called()

    def test_waiting_flag_reset_when_request_available(self):
        s = self._make_ready_scheduler()
        s.waiting = True
        from scrapy.http import Request
        s.queue.pop.return_value = Request("http://example.com/")
        s.next_request()
        assert s.waiting is False


# ---------------------------------------------------------------------------
# has_pending_requests
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerHasPending:
    def test_has_pending_returns_true_when_not_closing(self):
        s = _make_scheduler()
        s.closing = False
        assert s.has_pending_requests() is True

    def test_has_pending_returns_false_when_closing(self):
        s = _make_scheduler()
        s.closing = True
        assert s.has_pending_requests() is False


# ---------------------------------------------------------------------------
# push/pop helpers
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerPushPop:
    def _make_ready_scheduler(self):
        s = _make_scheduler()
        s.queue = MagicMock()
        s.spider = _make_spider()
        return s

    def test_push_distributed_queue_returns_true_when_queue_set(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        result = s.push_distributed_queue(Request("http://example.com/"))
        assert result is True
        s.queue.push.assert_called_once()

    def test_push_distributed_queue_returns_false_when_no_queue(self):
        s = _make_scheduler()
        s.queue = None
        from scrapy.http import Request
        result = s.push_distributed_queue(Request("http://example.com/"))
        assert result is False

    def test_pop_distributed_queue_delegates_to_queue(self):
        s = self._make_ready_scheduler()
        from scrapy.http import Request
        expected = Request("http://example.com/")
        s.queue.pop.return_value = expected
        result = s.pop_distributed_queue()
        assert result is expected


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerClose:
    def test_close_delegates_to_dupefilter(self):
        s = _make_scheduler()
        s.df = MagicMock()
        s.close("finished")
        s.df.close.assert_called_once_with("finished")

    def test_close_returns_dupefilter_result(self):
        s = _make_scheduler()
        s.df = MagicMock()
        s.df.close.return_value = "done"
        result = s.close("finished")
        assert result == "done"


# ---------------------------------------------------------------------------
# requeue_message
# ---------------------------------------------------------------------------

class TestDistributedQueueSchedulerRequeueMessage:
    def test_requeue_message_pushes_to_queue(self):
        s = _make_scheduler()
        s.queue = MagicMock()
        s.requeue_message("http://example.com/")
        s.queue.push.assert_called_once_with("http://example.com/", None)

    def test_requeue_message_with_headers_pushes_headers(self):
        s = _make_scheduler()
        s.queue = MagicMock()
        headers = {"x-retry": "1"}
        s.requeue_message("http://example.com/", headers=headers)
        s.queue.push.assert_called_once_with("http://example.com/", headers)

