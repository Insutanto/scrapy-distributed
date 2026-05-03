"""
Unit tests for scrapy_distributed.queues.kafka.KafkaQueue.

All Kafka client interactions are mocked – no broker is required.
"""
import json
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from scrapy_distributed.queues.kafka import KafkaQueue
from scrapy_distributed.common.queue_config import KafkaQueueConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_queue(**kwargs):
    """Return a KafkaQueue with mocked internals (no real broker)."""
    defaults = dict(
        connection_conf="localhost:9092",
        name="test-topic",
    )
    defaults.update(kwargs)
    with patch("scrapy_distributed.queues.kafka.KafkaAdminClient") as MockAdmin, \
         patch("scrapy_distributed.queues.kafka.KafkaProducer") as MockProducer, \
         patch("scrapy_distributed.queues.kafka.KafkaConsumer") as MockConsumer:
        MockAdmin.return_value = MagicMock()
        MockProducer.return_value = MagicMock()
        MockConsumer.return_value = MagicMock()
        q = KafkaQueue(**defaults)
    return q


def _make_scheduler(spider_name="test_spider"):
    spider = SimpleNamespace(name=spider_name)
    return SimpleNamespace(spider=spider)


# ---------------------------------------------------------------------------
# Construction & from_queue_conf
# ---------------------------------------------------------------------------

class TestKafkaQueueConstruction:
    def test_topic_derived_from_name(self):
        q = _make_queue(name="my-spider")
        assert q.name == "my-spider"
        assert q.topic == "my-spider.spider.queue"

    def test_attributes_set_correctly(self):
        q = _make_queue(name="crawl", num_partitions=5, replication_factor=2)
        assert q.num_partitions == 5
        assert q.replication_factor == 2

    def test_from_queue_conf(self):
        conf = KafkaQueueConfig(
            topic="events",
            num_partitions=4,
            replication_factor=1,
            arguments={"compression": "gzip"},
        )
        with patch("scrapy_distributed.queues.kafka.KafkaAdminClient") as MockAdmin, \
             patch("scrapy_distributed.queues.kafka.KafkaProducer"), \
             patch("scrapy_distributed.queues.kafka.KafkaConsumer"):
            MockAdmin.return_value = MagicMock()
            q = KafkaQueue.from_queue_conf("localhost:9092", conf)
        assert q.name == "events"
        assert q.num_partitions == 4
        assert q.replication_factor == 1

    def test_connect_called_on_init(self):
        with patch("scrapy_distributed.queues.kafka.KafkaAdminClient") as MockAdmin, \
             patch("scrapy_distributed.queues.kafka.KafkaProducer") as MockProducer, \
             patch("scrapy_distributed.queues.kafka.KafkaConsumer") as MockConsumer:
            MockAdmin.return_value = MagicMock()
            MockProducer.return_value = MagicMock()
            MockConsumer.return_value = MagicMock()
            KafkaQueue(connection_conf="localhost:9092", name="t")
        MockAdmin.assert_called_once()
        MockProducer.assert_called_once()
        MockConsumer.assert_called_once()


# ---------------------------------------------------------------------------
# __len__
# ---------------------------------------------------------------------------

class TestKafkaQueueLen:
    def test_len_returns_one(self):
        """KafkaQueue.__len__ always returns 1 (by design)."""
        q = _make_queue()
        assert len(q) == 1


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------

class TestKafkaQueuePop:
    def test_pop_returns_none_when_no_messages(self):
        q = _make_queue()
        q.consumer.poll.return_value = {}
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is None

    def test_pop_returns_request_from_message(self):
        q = _make_queue()
        body = json.dumps({"url": "http://example.com/page", "method": "GET"}).encode()
        mock_msg = MagicMock()
        mock_msg.value = body
        mock_partition = MagicMock()
        mock_partition.__iter__ = MagicMock(return_value=iter([mock_msg]))
        q.consumer.poll.return_value = {"part": [mock_msg]}
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is not None
        assert result.url == "http://example.com/page"

    def test_pop_returns_none_when_body_is_empty(self):
        q = _make_queue()
        q.consumer.poll.return_value = {}
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is None

    def test_pop_handles_consumer_exception(self):
        q = _make_queue()
        q.consumer.poll.side_effect = Exception("consumer error")
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is None


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------

class TestKafkaQueuePush:
    def _make_request(self, url="http://example.com/"):
        from scrapy.http import Request
        return Request(url)

    def test_push_calls_producer_send(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        request = self._make_request()
        q.push(request, scheduler)
        q.producer.send.assert_called_once()

    def test_push_sends_to_correct_topic(self):
        q = _make_queue(name="my-spider")
        scheduler = _make_scheduler()
        request = self._make_request()
        q.push(request, scheduler)
        args, _ = q.producer.send.call_args
        assert args[0] == "my-spider.spider.queue"

    def test_push_encodes_url_in_body(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        request = self._make_request("http://example.com/target")
        q.push(request, scheduler)
        args, kwargs = q.producer.send.call_args
        body_bytes = args[1]
        body = json.loads(body_bytes.decode())
        assert body["url"] == "http://example.com/target"


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestKafkaQueueClose:
    def test_close_shuts_down_all_clients(self):
        q = _make_queue()
        q.close()
        q.admin_client.close.assert_called_once()
        q.producer.close.assert_called_once()
        q.consumer.close.assert_called_once()


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

class TestKafkaQueueConnect:
    def test_connect_recreates_clients(self):
        q = _make_queue()
        old_producer = q.producer
        old_consumer = q.consumer
        with patch("scrapy_distributed.queues.kafka.KafkaAdminClient") as MockAdmin, \
             patch("scrapy_distributed.queues.kafka.KafkaProducer") as MockProducer, \
             patch("scrapy_distributed.queues.kafka.KafkaConsumer") as MockConsumer:
            MockAdmin.return_value = MagicMock()
            MockProducer.return_value = MagicMock()
            MockConsumer.return_value = MagicMock()
            q.connect()
        # old clients should have been closed
        old_producer.close.assert_called()
        old_consumer.close.assert_called()


# ---------------------------------------------------------------------------
# _request_to_dict / _request_from_dict (round-trip)
# ---------------------------------------------------------------------------

class TestKafkaQueueSerialisation:
    def test_round_trip_preserves_url(self):
        from scrapy.http import Request
        request = Request("http://example.com/roundtrip")
        d = KafkaQueue._request_to_dict(request)
        restored = KafkaQueue._request_from_dict(d)
        assert restored.url == "http://example.com/roundtrip"

    def test_request_to_dict_excludes_falsy_values(self):
        from scrapy.http import Request
        request = Request("http://example.com/")
        d = KafkaQueue._request_to_dict(request)
        for v in d.values():
            assert v
