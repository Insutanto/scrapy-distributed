"""
Unit tests for scrapy_distributed.queues.rocketmq.RocketMQQueue.

All RocketMQ client interactions are mocked so no broker is required.
"""

import json
import sys
import types
from queue import Queue
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Stub out the rocketmq package before importing the module under test so
# that the tests work even if rocketmq-client-python is not installed.
# ---------------------------------------------------------------------------
_rocketmq_stub = types.ModuleType("rocketmq")
_rocketmq_client_stub = types.ModuleType("rocketmq.client")

ConsumeStatus = SimpleNamespace(CONSUME_SUCCESS="OK", RECONSUME_LATER="LATER")
_rocketmq_client_stub.ConsumeStatus = ConsumeStatus
_rocketmq_client_stub.Producer = MagicMock
_rocketmq_client_stub.PushConsumer = MagicMock
_rocketmq_client_stub.Message = MagicMock

sys.modules.setdefault("rocketmq", _rocketmq_stub)
sys.modules.setdefault("rocketmq.client", _rocketmq_client_stub)

from scrapy_distributed.queues.rocketmq import RocketMQQueue  # noqa: E402
from scrapy_distributed.common.queue_config import RocketMQQueueConfig  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_queue(**kwargs):
    """Return a RocketMQQueue with mocked internals (no real broker)."""
    defaults = dict(name_server="127.0.0.1:9876", topic="test-topic", group="test-group")
    defaults.update(kwargs)
    with patch.object(RocketMQQueue, "connect", lambda self: None):
        q = RocketMQQueue(**defaults)
    # Replace internals with fresh mocks
    q.producer = MagicMock()
    q.consumer = MagicMock()
    return q


def _make_scheduler(spider_name="test_spider"):
    """Return a minimal scheduler stub."""
    spider = SimpleNamespace(name=spider_name)
    return SimpleNamespace(spider=spider)


def _request_body(url="http://example.com/", **extra):
    """Return a minimal serialised request body as bytes."""
    d = {"url": url, "method": "GET"}
    d.update(extra)
    return json.dumps(d).encode()


# ---------------------------------------------------------------------------
# Construction & from_queue_conf
# ---------------------------------------------------------------------------

class TestRocketMQQueueConstruction:
    def test_attributes_set_correctly(self):
        q = _make_queue(tags="mytag", keys="mykey")
        assert q.name_server == "127.0.0.1:9876"
        assert q.name == "test-topic"
        assert q.topic == "test-topic"
        assert q.group == "test-group"
        assert q.tags == "mytag"
        assert q.keys == "mykey"

    def test_from_queue_conf(self):
        conf = RocketMQQueueConfig(
            topic="orders",
            group="order_group",
            tags="urgent",
            keys="order_id",
        )
        with patch.object(RocketMQQueue, "connect", lambda self: None):
            q = RocketMQQueue.from_queue_conf("127.0.0.1:9876", conf)
        q.producer = MagicMock()
        q.consumer = MagicMock()
        assert q.topic == "orders"
        assert q.group == "order_group"
        assert q.tags == "urgent"
        assert q.keys == "order_id"


# ---------------------------------------------------------------------------
# __len__
# ---------------------------------------------------------------------------

class TestRocketMQQueueLen:
    def test_empty_buffer_returns_zero(self):
        q = _make_queue()
        assert len(q) == 0

    def test_len_reflects_buffer_size(self):
        q = _make_queue()
        q._msg_buffer.put(b"msg1")
        q._msg_buffer.put(b"msg2")
        assert len(q) == 2


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------

class TestRocketMQQueuePop:
    def test_pop_returns_none_when_buffer_empty(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        assert q.pop(scheduler) is None

    def test_pop_returns_request_from_buffer(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        body = _request_body("http://example.com/page")
        q._msg_buffer.put(body)
        request = q.pop(scheduler)
        assert request is not None
        assert request.url == "http://example.com/page"

    def test_pop_drains_buffer_in_fifo_order(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        urls = ["http://example.com/1", "http://example.com/2", "http://example.com/3"]
        for url in urls:
            q._msg_buffer.put(_request_body(url))
        popped = [q.pop(scheduler).url for _ in urls]
        assert popped == urls

    def test_pop_accepts_string_body(self):
        """Body may arrive as str instead of bytes."""
        q = _make_queue()
        scheduler = _make_scheduler()
        body = json.dumps({"url": "http://example.com/str", "method": "GET"})
        q._msg_buffer.put(body)
        request = q.pop(scheduler)
        assert request.url == "http://example.com/str"


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------

class TestRocketMQQueuePush:
    def _make_request(self, url="http://example.com/"):
        from scrapy.http import Request
        return Request(url)

    def test_push_sends_sync(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        request = self._make_request()
        with patch("scrapy_distributed.queues.rocketmq.Message") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            q.push(request, scheduler)
        q.producer.send_sync.assert_called_once_with(mock_msg)

    def test_push_sets_tags_when_configured(self):
        q = _make_queue(tags="important")
        scheduler = _make_scheduler()
        request = self._make_request()
        with patch("scrapy_distributed.queues.rocketmq.Message") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            q.push(request, scheduler)
        mock_msg.set_tags.assert_called_once_with("important")

    def test_push_does_not_set_tags_when_none(self):
        q = _make_queue(tags=None)
        scheduler = _make_scheduler()
        request = self._make_request()
        with patch("scrapy_distributed.queues.rocketmq.Message") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            q.push(request, scheduler)
        mock_msg.set_tags.assert_not_called()

    def test_push_sets_keys_when_configured(self):
        q = _make_queue(keys="order-123")
        scheduler = _make_scheduler()
        request = self._make_request()
        with patch("scrapy_distributed.queues.rocketmq.Message") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            q.push(request, scheduler)
        mock_msg.set_keys.assert_called_once_with("order-123")

    def test_push_encodes_request_url_in_body(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        request = self._make_request("http://example.com/target")
        captured = {}
        with patch("scrapy_distributed.queues.rocketmq.Message") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            mock_msg.set_body.side_effect = lambda b: captured.update({"body": b})
            q.push(request, scheduler)
        body_dict = json.loads(captured["body"].decode())
        assert body_dict["url"] == "http://example.com/target"


# ---------------------------------------------------------------------------
# _on_message (consumer callback)
# ---------------------------------------------------------------------------

class TestRocketMQQueueOnMessage:
    def test_on_message_buffers_body_and_returns_success(self):
        q = _make_queue()
        msg = SimpleNamespace(body=b'{"url": "http://example.com/", "method": "GET"}')
        result = q._on_message(msg)
        assert result == ConsumeStatus.CONSUME_SUCCESS
        assert not q._msg_buffer.empty()
        assert q._msg_buffer.get_nowait() == msg.body

    def test_on_message_returns_reconsume_on_error(self):
        q = _make_queue()

        class BadQueue:
            def put(self, item):
                raise RuntimeError("disk full")

        q._msg_buffer = BadQueue()
        msg = SimpleNamespace(body=b"data")
        result = q._on_message(msg)
        assert result == ConsumeStatus.RECONSUME_LATER


# ---------------------------------------------------------------------------
# connect / close
# ---------------------------------------------------------------------------

class TestRocketMQQueueConnect:
    def test_connect_creates_producer_and_consumer(self):
        with patch("scrapy_distributed.queues.rocketmq.Producer") as MockProducer, \
             patch("scrapy_distributed.queues.rocketmq.PushConsumer") as MockConsumer:
            mock_producer = MagicMock()
            mock_consumer = MagicMock()
            MockProducer.return_value = mock_producer
            MockConsumer.return_value = mock_consumer

            q = _make_queue()
            q.producer = None
            q.consumer = None
            q.connect()

        MockProducer.assert_called_once_with("test-group")
        mock_producer.set_name_server_address.assert_called_once_with("127.0.0.1:9876")
        mock_producer.start.assert_called_once()

        MockConsumer.assert_called_once_with("test-group.spider.consumer")
        mock_consumer.set_name_server_address.assert_called_once_with("127.0.0.1:9876")
        mock_consumer.subscribe.assert_called_once_with("test-topic", q._on_message)
        mock_consumer.start.assert_called_once()

    def test_connect_shuts_down_existing_connections(self):
        q = _make_queue()
        old_producer = q.producer
        old_consumer = q.consumer

        with patch("scrapy_distributed.queues.rocketmq.Producer") as MockProducer, \
             patch("scrapy_distributed.queues.rocketmq.PushConsumer") as MockConsumer:
            MockProducer.return_value = MagicMock()
            MockConsumer.return_value = MagicMock()
            q.connect()

        old_producer.shutdown.assert_called_once()
        old_consumer.shutdown.assert_called_once()


class TestRocketMQQueueClose:
    def test_close_shuts_down_producer_and_consumer(self):
        q = _make_queue()
        q.close()
        q.producer.shutdown.assert_called_once()
        q.consumer.shutdown.assert_called_once()

    def test_close_tolerates_missing_producer(self):
        q = _make_queue()
        q.producer = None
        q.close()  # must not raise

    def test_close_tolerates_missing_consumer(self):
        q = _make_queue()
        q.consumer = None
        q.close()  # must not raise

    def test_close_tolerates_shutdown_exception(self):
        q = _make_queue()
        q.producer.shutdown.side_effect = RuntimeError("already stopped")
        q.consumer.shutdown.side_effect = RuntimeError("already stopped")
        q.close()  # must not raise


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------

class TestRocketMQQueueClear:
    def test_clear_empties_buffer(self):
        q = _make_queue()
        for i in range(5):
            q._msg_buffer.put(f"msg{i}".encode())
        q.clear()
        assert q._msg_buffer.empty()

    def test_clear_on_empty_buffer_is_a_noop(self):
        q = _make_queue()
        q.clear()  # must not raise
        assert q._msg_buffer.empty()
