"""
Unit tests for scrapy_distributed middlewares:
  - RabbitMiddleware (middlewares/amqp.py)
  - KafkaMiddleware  (middlewares/kafka.py)

All external interactions are mocked – no real broker required.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from scrapy.http import Request, Response, TextResponse


# ===========================================================================
# Helpers
# ===========================================================================

def _make_settings(**kwargs):
    defaults = {"SCHEDULER_REQUEUE_ON_STATUS": []}
    defaults.update(kwargs)
    settings = MagicMock()
    settings.get = lambda k, default=None: defaults.get(k, default)
    return settings


def _make_spider(name="myspider"):
    spider = MagicMock()
    spider.name = name
    spider.logger = MagicMock()
    return spider


def _make_request(url="http://example.com/", delivery_tag=None):
    meta = {}
    if delivery_tag is not None:
        meta["delivery_tag"] = delivery_tag
    return Request(url, meta=meta)


def _make_response(url="http://example.com/", status=200):
    return TextResponse(url=url, status=status, body=b"<html/>")


def _make_response_image(ext=".jpg"):
    return TextResponse(url=f"http://example.com/image{ext}", status=200, body=b"data")


# ===========================================================================
# RabbitMiddleware
# ===========================================================================

from scrapy_distributed.middlewares.amqp import RabbitMiddleware


class TestRabbitMiddlewareInit:
    def test_default_requeue_list_is_empty(self):
        mw = RabbitMiddleware(_make_settings())
        assert mw.requeue_list == []

    def test_custom_requeue_list(self):
        mw = RabbitMiddleware(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=[500, 503]))
        assert mw.requeue_list == [500, 503]

    def test_init_flag_set(self):
        mw = RabbitMiddleware(_make_settings())
        assert mw.init is True

    def test_from_crawler(self):
        crawler = MagicMock()
        crawler.settings = _make_settings(SCHEDULER_REQUEUE_ON_STATUS=[502])
        mw = RabbitMiddleware.from_crawler(crawler)
        assert mw.requeue_list == [502]

    def test_from_settings(self):
        mw = RabbitMiddleware.from_settings(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=[404]))
        assert mw.requeue_list == [404]


class TestRabbitMiddlewareProcessRequest:
    def test_process_request_returns_none(self):
        mw = RabbitMiddleware(_make_settings())
        spider = _make_spider()
        request = _make_request()
        assert mw.process_request(request, spider) is None


class TestRabbitMiddlewareEnsureInit:
    def _make_initialised(self, requeue_list=None):
        mw = RabbitMiddleware(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=requeue_list or []))
        mock_scheduler = MagicMock()
        spider = _make_spider()
        spider.crawler.engine.slot.scheduler = mock_scheduler
        spider.crawler.stats = MagicMock()
        mw.ensure_init(spider)
        return mw, spider, mock_scheduler

    def test_ensure_init_sets_spider(self):
        mw, spider, _ = self._make_initialised()
        assert mw.spider is spider

    def test_ensure_init_sets_scheduler(self):
        mw, _, scheduler = self._make_initialised()
        assert mw.scheduler is scheduler

    def test_ensure_init_clears_flag(self):
        mw, _, _ = self._make_initialised()
        assert mw.init is False

    def test_ensure_init_is_idempotent(self):
        mw, spider, scheduler = self._make_initialised()
        # Second call should be a no-op
        new_spider = _make_spider("another")
        mw.ensure_init(new_spider)
        assert mw.spider is spider  # unchanged


class TestRabbitMiddlewareHasDeliveryTag:
    def _init_mw(self):
        mw = RabbitMiddleware(_make_settings())
        mw.spider = _make_spider()
        mw.scheduler = MagicMock()
        mw.stats = MagicMock()
        mw.init = False
        return mw

    def test_returns_true_when_delivery_tag_present(self):
        mw = self._init_mw()
        request = _make_request(delivery_tag=42)
        assert mw.has_delivery_tag(request) is True

    def test_returns_false_when_no_delivery_tag(self):
        mw = self._init_mw()
        request = _make_request()
        assert mw.has_delivery_tag(request) is False


class TestRabbitMiddlewareAck:
    def _init_mw(self):
        mw = RabbitMiddleware(_make_settings())
        mw.spider = _make_spider()
        mw.scheduler = MagicMock()
        mw.stats = MagicMock()
        mw.init = False
        return mw

    def test_ack_calls_queue_ack_when_delivery_tag_present(self):
        mw = self._init_mw()
        request = _make_request(delivery_tag=99)
        response = _make_response()
        mw.ack(request, response)
        mw.scheduler.queue.ack.assert_called_once_with(99)

    def test_ack_does_not_call_queue_ack_when_no_delivery_tag(self):
        mw = self._init_mw()
        request = _make_request()  # no delivery_tag
        response = _make_response()
        mw.ack(request, response)
        mw.scheduler.queue.ack.assert_not_called()

    def test_ack_increments_stat(self):
        mw = self._init_mw()
        request = _make_request(delivery_tag=1)
        response = _make_response()
        mw.ack(request, response)
        mw.stats.inc_value.assert_called()


class TestRabbitMiddlewareProcessResponse:
    def _init_mw(self, requeue_list=None):
        mw = RabbitMiddleware(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=requeue_list or []))
        spider = _make_spider()
        mw.spider = spider
        mw.scheduler = MagicMock()
        mw.stats = MagicMock()
        mw.init = False
        # RedirectMiddleware.process_response needs self.crawler
        mw.crawler = MagicMock()
        mw.crawler.spider.handle_httpstatus_list = []
        return mw, spider

    def test_normal_response_is_acked_and_returned(self):
        mw, spider = self._init_mw()
        request = _make_request(delivery_tag=1)
        response = _make_response()
        result = mw.process_response(request, response, spider)
        assert result is response
        mw.scheduler.queue.ack.assert_called_once_with(1)

    def test_image_response_is_not_acked(self):
        mw, spider = self._init_mw()
        request = _make_request(delivery_tag=1)
        response = _make_response_image(".jpg")
        mw.process_response(request, response, spider)
        mw.scheduler.queue.ack.assert_not_called()

    def test_status_in_requeue_list_causes_requeue_and_ignore(self):
        from scrapy.exceptions import IgnoreRequest
        mw, spider = self._init_mw(requeue_list=[503])
        request = _make_request(delivery_tag=1)
        response = _make_response(status=503)
        with pytest.raises(IgnoreRequest):
            mw.process_response(request, response, spider)
        mw.scheduler.requeue_message.assert_called()

    def test_redirect_response_is_acked_and_new_request_returned(self):
        mw, spider = self._init_mw()
        mw.max_redirect_times = 20  # needed by RedirectMiddleware._redirect
        request = _make_request(delivery_tag=1, url="http://example.com/")
        redirect_response = TextResponse(
            url="http://example.com/",
            status=301,
            headers={"Location": "http://example.com/new"},
            body=b"",
        )
        result = mw.process_response(request, redirect_response, spider)
        # RedirectMiddleware returns a new Request for 301, and delivery_tag is acked
        assert isinstance(result, Request)
        mw.scheduler.queue.ack.assert_called_once_with(1)


# ===========================================================================
# KafkaMiddleware
# ===========================================================================

from scrapy_distributed.middlewares.kafka import KafkaMiddleware


class TestKafkaMiddlewareInit:
    def test_default_requeue_list_is_empty(self):
        mw = KafkaMiddleware(_make_settings())
        assert mw.requeue_list == []

    def test_custom_requeue_list(self):
        mw = KafkaMiddleware(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=[500, 503]))
        assert mw.requeue_list == [500, 503]

    def test_init_flag_set(self):
        mw = KafkaMiddleware(_make_settings())
        assert mw.init is True

    def test_from_crawler(self):
        crawler = MagicMock()
        crawler.settings = _make_settings(SCHEDULER_REQUEUE_ON_STATUS=[503])
        mw = KafkaMiddleware.from_crawler(crawler)
        assert mw.requeue_list == [503]


class TestKafkaMiddlewareProcessRequest:
    def test_process_request_returns_none(self):
        mw = KafkaMiddleware(_make_settings())
        spider = _make_spider()
        request = _make_request()
        assert mw.process_request(request, spider) is None


class TestKafkaMiddlewareEnsureInit:
    def _make_initialised(self):
        mw = KafkaMiddleware(_make_settings())
        mock_scheduler = MagicMock()
        spider = _make_spider()
        spider.crawler.engine.slot.scheduler = mock_scheduler
        spider.crawler.stats = MagicMock()
        mw.ensure_init(spider)
        return mw, spider, mock_scheduler

    def test_ensure_init_sets_scheduler(self):
        mw, _, scheduler = self._make_initialised()
        assert mw.scheduler is scheduler

    def test_ensure_init_clears_flag(self):
        mw, _, _ = self._make_initialised()
        assert mw.init is False


class TestKafkaMiddlewareProcessResponse:
    def _init_mw(self, requeue_list=None):
        mw = KafkaMiddleware(_make_settings(SCHEDULER_REQUEUE_ON_STATUS=requeue_list or []))
        spider = _make_spider()
        mw.spider = spider
        mw.scheduler = MagicMock()
        mw.stats = MagicMock()
        mw.init = False
        return mw, spider

    def test_normal_response_is_returned(self):
        mw, spider = self._init_mw()
        request = _make_request()
        response = _make_response()
        result = mw.process_response(request, response, spider)
        assert result is response

    def test_image_response_calls_process_picture(self):
        mw, spider = self._init_mw()
        request = _make_request()
        response = _make_response_image(".png")
        mw.stats = MagicMock()
        mw.process_response(request, response, spider)
        # process_picture calls inc_stat("picture")
        mw.stats.inc_value.assert_called()

    def test_status_in_requeue_list_causes_ignore_request(self):
        from scrapy.exceptions import IgnoreRequest
        mw, spider = self._init_mw(requeue_list=[502])
        request = _make_request()
        response = _make_response(status=502)
        with pytest.raises(IgnoreRequest):
            mw.process_response(request, response, spider)
        mw.scheduler.requeue_message.assert_called()

    def test_requeue_sets_meta_flag(self):
        from scrapy.exceptions import IgnoreRequest
        mw, spider = self._init_mw(requeue_list=[503])
        request = _make_request()
        response = _make_response(status=503)
        try:
            mw.process_response(request, response, spider)
        except Exception:
            pass
        assert request.meta.get("requeued") is True
