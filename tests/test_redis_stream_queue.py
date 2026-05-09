"""
Unit tests for scrapy_distributed.queues.redis_stream.RedisStreamQueue.

All Redis interactions are mocked – no server is required.
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scrapy_distributed.queues.redis_stream import RedisStreamQueue
from scrapy_distributed.common.queue_config import RedisStreamQueueConfig


def _make_queue(**kwargs):
    defaults = dict(connection_conf="redis://localhost:6379/0", name="test-stream")
    defaults.update(kwargs)
    with patch("scrapy_distributed.queues.redis_stream.get_redis") as mock_get_redis:
        mock_get_redis.return_value = MagicMock()
        q = RedisStreamQueue(**defaults)
    return q


def _make_scheduler(spider_name="test_spider"):
    spider = SimpleNamespace(name=spider_name)
    return SimpleNamespace(spider=spider)


class TestRedisStreamQueueConstruction:
    def test_attributes_set_correctly(self):
        q = _make_queue(name="my-stream", id="0-0", maxlen=100, approximate=False)
        assert q.name == "my-stream"
        assert q.id == "0-0"
        assert q.maxlen == 100
        assert q.approximate is False

    def test_from_queue_conf(self):
        conf = RedisStreamQueueConfig(
            name="crawl-stream",
            id="0-0",
            maxlen=1000,
            approximate=False,
            arguments={"nomkstream": True},
        )
        with patch("scrapy_distributed.queues.redis_stream.get_redis") as mock_get_redis:
            mock_get_redis.return_value = MagicMock()
            q = RedisStreamQueue.from_queue_conf("redis://localhost:6379/0", conf)
        assert q.name == "crawl-stream"
        assert q.id == "0-0"
        assert q.maxlen == 1000
        assert q.approximate is False
        assert q.arguments == {"nomkstream": True}


class TestRedisStreamQueueLen:
    def test_len_delegates_to_xlen(self):
        q = _make_queue()
        q.redis_client.xlen.return_value = 7
        assert len(q) == 7


class TestRedisStreamQueuePop:
    def test_pop_returns_none_when_empty(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        q.redis_client.xread.return_value = []
        assert q.pop(scheduler) is None

    def test_pop_returns_request_and_deletes_message(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        q.redis_client.xread.return_value = [
            (
                "test-stream",
                [("1-0", {"data": json.dumps({"url": "http://example.com/", "method": "GET"})})],
            )
        ]
        req = q.pop(scheduler)
        assert req is not None
        assert req.url == "http://example.com/"
        q.redis_client.xdel.assert_called_once_with("test-stream", "1-0")
        assert q.last_id == "1-0"

    def test_pop_returns_none_when_message_has_no_data_field(self):
        q = _make_queue()
        scheduler = _make_scheduler()
        q.redis_client.xread.return_value = [("test-stream", [("1-0", {"other": "x"})])]
        assert q.pop(scheduler) is None


class TestRedisStreamQueuePush:
    def test_push_calls_xadd(self):
        from scrapy.http import Request

        q = _make_queue()
        scheduler = _make_scheduler()
        q.push(Request("http://example.com/"), scheduler)
        q.redis_client.xadd.assert_called_once()

    def test_push_uses_config_options(self):
        from scrapy.http import Request

        q = _make_queue(id="0-0", maxlen=100, approximate=False, arguments={"nomkstream": True})
        scheduler = _make_scheduler()
        q.push(Request("http://example.com/"), scheduler)
        _, kwargs = q.redis_client.xadd.call_args
        assert kwargs["id"] == "0-0"
        assert kwargs["maxlen"] == 100
        assert kwargs["approximate"] is False
        assert kwargs["nomkstream"] is True


class TestRedisStreamQueueConnection:
    def test_connect_uses_url_when_connection_is_string(self):
        q = _make_queue(connection_conf="redis://localhost:6379/0")
        with patch("scrapy_distributed.queues.redis_stream.get_redis") as mock_get_redis:
            mock_get_redis.return_value = MagicMock()
            q.connect()
        mock_get_redis.assert_called_once_with(url="redis://localhost:6379/0")

    def test_connect_uses_kwargs_when_connection_is_dict(self):
        q = _make_queue(connection_conf={"host": "localhost", "port": 6379})
        with patch("scrapy_distributed.queues.redis_stream.get_redis") as mock_get_redis:
            mock_get_redis.return_value = MagicMock()
            q.connect()
        mock_get_redis.assert_called_once_with(host="localhost", port=6379)

    def test_close_calls_client_close_when_available(self):
        q = _make_queue()
        q.close()
        q.redis_client.close.assert_called_once()

    def test_clear_deletes_stream_key(self):
        q = _make_queue(name="stream-x")
        q.clear()
        q.redis_client.delete.assert_called_once_with("stream-x")
