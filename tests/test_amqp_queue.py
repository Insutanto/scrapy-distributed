"""
Unit tests for scrapy_distributed.queues.amqp.RabbitQueue.

All pika / AMQP interactions are mocked – no broker is required.
"""
import json
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from scrapy_distributed.queues.amqp import RabbitQueue
from scrapy_distributed.common.queue_config import RabbitQueueConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_queue(**kwargs):
    """Return a RabbitQueue with mocked internals (no real broker)."""
    defaults = dict(
        connection_url="amqp://guest:guest@localhost/",
        name="test-queue",
    )
    defaults.update(kwargs)
    with patch("scrapy_distributed.queues.amqp.connection") as mock_conn:
        mock_conn.connect.return_value = MagicMock()
        mock_conn.get_channel.return_value = MagicMock()
        q = RabbitQueue(**defaults)
    return q


def _make_scheduler(spider_name="test_spider"):
    spider = SimpleNamespace(name=spider_name)
    return SimpleNamespace(spider=spider)


def _request_body(url="http://example.com/"):
    return json.dumps({"url": url, "method": "GET"}).encode()


# ---------------------------------------------------------------------------
# Construction & from_queue_conf
# ---------------------------------------------------------------------------

class TestRabbitQueueConstruction:
    def test_attributes_set_correctly(self):
        q = _make_queue(name="myqueue", durable=True)
        assert q.name == "myqueue"
        assert q.durable is True
        assert q.connection_url == "amqp://guest:guest@localhost/"

    def test_connect_called_on_init(self):
        with patch("scrapy_distributed.queues.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            RabbitQueue(connection_url="amqp://localhost/", name="q")
        mock_conn.connect.assert_called_once()
        mock_conn.get_channel.assert_called_once()

    def test_from_queue_conf(self):
        conf = RabbitQueueConfig(
            name="orders",
            durable=True,
            passive=False,
            exclusive=False,
            auto_delete=True,
            arguments={"x-max-priority": 5},
            properties={"delivery_mode": 2},
        )
        with patch("scrapy_distributed.queues.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            q = RabbitQueue.from_queue_conf("amqp://localhost/", conf)
        assert q.name == "orders"
        assert q.durable is True
        assert q.auto_delete is True
        assert q.arguments == {"x-max-priority": 5}
        assert q.properties == {"delivery_mode": 2}


# ---------------------------------------------------------------------------
# __len__
# ---------------------------------------------------------------------------

class TestRabbitQueueLen:
    def test_len_returns_message_count(self):
        q = _make_queue()
        declared = MagicMock()
        declared.method.message_count = 7
        q.channel.queue_declare.return_value = declared
        assert len(q) == 7

    def test_len_zero_when_empty(self):
        q = _make_queue()
        declared = MagicMock()
        declared.method.message_count = 0
        q.channel.queue_declare.return_value = declared
        assert len(q) == 0


# ---------------------------------------------------------------------------
# pop
# ---------------------------------------------------------------------------

class TestRabbitQueuePop:
    def test_pop_returns_none_when_queue_empty(self):
        q = _make_queue()
        q.channel.basic_get.return_value = (None, None, None)
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is None

    def test_pop_returns_request_with_url(self):
        q = _make_queue()
        method = MagicMock()
        method.delivery_tag = 42
        body = _request_body("http://example.com/page")
        q.channel.basic_get.return_value = (method, MagicMock(), body)
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result is not None
        assert result.url == "http://example.com/page"

    def test_pop_stores_delivery_tag_in_meta(self):
        q = _make_queue()
        method = MagicMock()
        method.delivery_tag = 99
        body = _request_body("http://example.com/")
        q.channel.basic_get.return_value = (method, MagicMock(), body)
        scheduler = _make_scheduler()
        result = q.pop(scheduler)
        assert result.meta["delivery_tag"] == 99


# ---------------------------------------------------------------------------
# push
# ---------------------------------------------------------------------------

class TestRabbitQueuePush:
    def _make_request(self, url="http://example.com/"):
        from scrapy.http import Request
        return Request(url)

    def test_push_calls_basic_publish(self):
        q = _make_queue(properties={"delivery_mode": 1})
        scheduler = _make_scheduler()
        request = self._make_request()
        q.push(request, scheduler)
        q.channel.basic_publish.assert_called_once()

    def test_push_uses_queue_name_as_routing_key(self):
        q = _make_queue(name="myroute", properties={"delivery_mode": 1})
        scheduler = _make_scheduler()
        request = self._make_request()
        q.push(request, scheduler)
        _, kwargs = q.channel.basic_publish.call_args
        assert kwargs["routing_key"] == "myroute"

    def test_push_with_headers_includes_them_in_properties(self):
        q = _make_queue(properties={"delivery_mode": 2})
        scheduler = _make_scheduler()
        request = self._make_request()
        q.push(request, scheduler, headers={"x-retry": 1})
        _, kwargs = q.channel.basic_publish.call_args
        props = kwargs["properties"]
        assert props.headers == {"x-retry": 1}

    def test_push_encodes_url_in_body(self):
        q = _make_queue(properties={"delivery_mode": 1})
        scheduler = _make_scheduler()
        request = self._make_request("http://example.com/target")
        q.push(request, scheduler)
        _, kwargs = q.channel.basic_publish.call_args
        body = json.loads(kwargs["body"])
        assert body["url"] == "http://example.com/target"


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------

class TestRabbitQueueAck:
    def test_ack_calls_basic_ack(self):
        q = _make_queue()
        q.ack(delivery_tag=55)
        q.channel.basic_ack.assert_called_once_with(delivery_tag=55)


# ---------------------------------------------------------------------------
# close / clear
# ---------------------------------------------------------------------------

class TestRabbitQueueClose:
    def test_close_calls_channel_close(self):
        q = _make_queue()
        q.close()
        q.channel.close.assert_called_once()


class TestRabbitQueueClear:
    def test_clear_calls_queue_purge(self):
        q = _make_queue()
        q.clear()
        q.channel.queue_purge.assert_called_once_with("test-queue")


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------

class TestRabbitQueueConnect:
    def test_connect_closes_existing_connection_first(self):
        q = _make_queue()
        old_connection = q.connection
        with patch("scrapy_distributed.queues.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            q.connect()
        old_connection.close.assert_called_once()

    def test_connect_skips_close_when_no_existing_connection(self):
        q = _make_queue()
        q.connection = None
        with patch("scrapy_distributed.queues.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            q.connect()  # should not raise


# ---------------------------------------------------------------------------
# _request_to_dict / _request_from_dict (round-trip)
# ---------------------------------------------------------------------------

class TestRabbitQueueSerialisation:
    def test_round_trip_preserves_url(self):
        from scrapy.http import Request
        request = Request("http://example.com/roundtrip")
        d = RabbitQueue._request_to_dict(request)
        restored = RabbitQueue._request_from_dict(d)
        assert restored.url == "http://example.com/roundtrip"

    def test_request_to_dict_excludes_falsy_values(self):
        from scrapy.http import Request
        request = Request("http://example.com/")
        d = RabbitQueue._request_to_dict(request)
        # All values in the dict should be truthy
        for v in d.values():
            assert v
