#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pika

from scrapy_distributed.common.queue_config import RabbitQueueConfig


def make_rabbit_queue(
    name="test_queue",
    exchange=None,
    exchange_type="direct",
    exchange_durable=True,
    exchange_arguments=None,
    properties=None,
):
    """Create a RabbitQueue with a mocked connection/channel."""
    from scrapy_distributed.queues.amqp import RabbitQueue

    mock_channel = MagicMock()
    mock_declare = MagicMock()
    mock_declare.method.message_count = 0
    mock_channel.queue_declare.return_value = mock_declare

    mock_connection = MagicMock()

    with patch("scrapy_distributed.amqp_utils.connection.connect", return_value=mock_connection), \
         patch("scrapy_distributed.amqp_utils.connection.get_channel", return_value=mock_channel):
        q = RabbitQueue(
            connection_url="amqp://guest:guest@localhost:5672/",
            name=name,
            exchange=exchange,
            exchange_type=exchange_type,
            exchange_durable=exchange_durable,
            exchange_arguments=exchange_arguments,
            properties=properties,
        )
    q.channel = mock_channel
    q.connection = mock_connection
    return q


def make_scheduler(spider=None):
    if spider is None:
        spider = SimpleNamespace(name="testspider")
    scheduler = SimpleNamespace(spider=spider)
    return scheduler


class TestRabbitQueueFromQueueConf:
    def test_fields_are_passed_from_queue_conf(self):
        from scrapy_distributed.queues.amqp import RabbitQueue

        conf = RabbitQueueConfig(
            name="myqueue",
            durable=True,
            exchange="myexchange",
            exchange_type="x-delayed-message",
            exchange_durable=False,
            exchange_arguments={"x-delayed-type": "direct"},
            properties={"delivery_mode": 2},
        )
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        with patch("scrapy_distributed.amqp_utils.connection.connect", return_value=mock_connection), \
             patch("scrapy_distributed.amqp_utils.connection.get_channel", return_value=mock_channel):
            q = RabbitQueue.from_queue_conf("amqp://localhost/", conf)

        assert q.name == "myqueue"
        assert q.durable is True
        assert q.exchange == "myexchange"
        assert q.exchange_type == "x-delayed-message"
        assert q.exchange_durable is False
        assert q.exchange_arguments == {"x-delayed-type": "direct"}
        assert q.properties == {"delivery_mode": 2}

    def test_defaults_when_no_exchange(self):
        from scrapy_distributed.queues.amqp import RabbitQueue

        conf = RabbitQueueConfig(name="simple")
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        with patch("scrapy_distributed.amqp_utils.connection.connect", return_value=mock_connection), \
             patch("scrapy_distributed.amqp_utils.connection.get_channel", return_value=mock_channel):
            q = RabbitQueue.from_queue_conf("amqp://localhost/", conf)

        assert q.exchange is None
        assert q.exchange_type == "direct"
        assert q.exchange_durable is True
        assert q.exchange_arguments is None


class TestRabbitQueueConnectWithExchange:
    def test_get_channel_called_with_exchange_params(self):
        from scrapy_distributed.queues.amqp import RabbitQueue

        mock_channel = MagicMock()
        mock_connection = MagicMock()

        with patch("scrapy_distributed.amqp_utils.connection.connect", return_value=mock_connection) as mock_connect, \
             patch("scrapy_distributed.amqp_utils.connection.get_channel", return_value=mock_channel) as mock_get_channel:
            RabbitQueue(
                connection_url="amqp://localhost/",
                name="myqueue",
                exchange="myexchange",
                exchange_type="x-delayed-message",
                exchange_durable=True,
                exchange_arguments={"x-delayed-type": "direct"},
            )

        mock_get_channel.assert_called_once_with(
            connection=mock_connection,
            queue="myqueue",
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=False,
            arguments=None,
            exchange="myexchange",
            exchange_type="x-delayed-message",
            exchange_durable=True,
            exchange_arguments={"x-delayed-type": "direct"},
        )


class TestRabbitQueuePushDelayedMessage:
    def _make_request(self, url="http://example.com", delay=None):
        meta = {}
        if delay is not None:
            meta["delay"] = delay
        request = SimpleNamespace(
            url=url,
            meta=meta,
            method="GET",
            headers={},
            body=b"",
            cookies={},
            encoding="utf-8",
            priority=0,
            dont_filter=True,
            flags=[],
            cb_kwargs={},
            callback=None,
            errback=None,
        )
        return request

    def test_push_without_delay_uses_default_exchange(self):
        q = make_rabbit_queue(name="myqueue")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(), scheduler)

        args, kwargs = q.channel.basic_publish.call_args
        assert kwargs.get("exchange", args[0] if args else "") == ""
        props = kwargs.get("properties") or args[2]
        assert props.headers is None or props.headers == {}

    def test_push_with_delay_adds_x_delay_header(self):
        q = make_rabbit_queue(name="myqueue")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(delay=5000), scheduler)

        args, kwargs = q.channel.basic_publish.call_args
        props = kwargs.get("properties")
        assert props is not None
        assert props.headers["x-delay"] == 5000

    def test_push_with_delay_as_float_is_cast_to_int(self):
        q = make_rabbit_queue(name="myqueue")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(delay=2500.7), scheduler)

        args, kwargs = q.channel.basic_publish.call_args
        props = kwargs.get("properties")
        assert isinstance(props.headers["x-delay"], int)
        assert props.headers["x-delay"] == 2500

    def test_push_uses_configured_exchange(self):
        q = make_rabbit_queue(name="myqueue", exchange="myexchange")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(), scheduler)

        _, kwargs = q.channel.basic_publish.call_args
        assert kwargs.get("exchange") == "myexchange"

    def test_push_with_delay_and_configured_exchange(self):
        q = make_rabbit_queue(
            name="myqueue",
            exchange="delayed_exchange",
            exchange_type="x-delayed-message",
            exchange_arguments={"x-delayed-type": "direct"},
        )
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(delay=3000), scheduler)

        _, kwargs = q.channel.basic_publish.call_args
        assert kwargs.get("exchange") == "delayed_exchange"
        assert kwargs.get("routing_key") == "myqueue"
        props = kwargs.get("properties")
        assert props.headers["x-delay"] == 3000

    def test_push_merges_extra_headers_with_delay(self):
        q = make_rabbit_queue(name="myqueue")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(delay=1000), scheduler, headers={"x-custom": "val"})

        _, kwargs = q.channel.basic_publish.call_args
        props = kwargs.get("properties")
        assert props.headers["x-delay"] == 1000
        assert props.headers["x-custom"] == "val"

    def test_push_without_delay_meta_does_not_set_x_delay(self):
        q = make_rabbit_queue(name="myqueue")
        scheduler = make_scheduler()

        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            q.push(self._make_request(), scheduler)

        _, kwargs = q.channel.basic_publish.call_args
        props = kwargs.get("properties")
        # No x-delay header when delay is not set
        assert props.headers is None or "x-delay" not in (props.headers or {})

    def test_properties_none_handled_gracefully(self):
        """RabbitQueue with no properties dict should not raise."""
        from scrapy_distributed.queues.amqp import RabbitQueue

        mock_channel = MagicMock()
        mock_connection = MagicMock()
        with patch("scrapy_distributed.amqp_utils.connection.connect", return_value=mock_connection), \
             patch("scrapy_distributed.amqp_utils.connection.get_channel", return_value=mock_channel):
            q = RabbitQueue(
                connection_url="amqp://localhost/",
                name="myqueue",
                properties=None,
            )
        q.channel = mock_channel

        scheduler = make_scheduler()
        with patch.object(q, "_request_to_dict", return_value={"url": "http://example.com"}):
            # Should not raise even though properties was initialized as None
            q.push(SimpleNamespace(url="http://example.com", meta={}), scheduler)

        _, kwargs = q.channel.basic_publish.call_args
        props = kwargs.get("properties")
        assert props.delivery_mode == 1


class TestConnectionGetChannelWithExchange:
    def test_exchange_declared_and_queue_bound_when_exchange_provided(self):
        from scrapy_distributed.amqp_utils.connection import get_channel

        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_declare = MagicMock()
        mock_declare.method.message_count = 0
        mock_channel.queue_declare.return_value = mock_declare

        get_channel(
            connection=mock_connection,
            queue="myqueue",
            exchange="myexchange",
            exchange_type="x-delayed-message",
            exchange_durable=True,
            exchange_arguments={"x-delayed-type": "direct"},
        )

        mock_channel.exchange_declare.assert_called_once_with(
            exchange="myexchange",
            exchange_type="x-delayed-message",
            durable=True,
            arguments={"x-delayed-type": "direct"},
        )
        mock_channel.queue_bind.assert_called_once_with(
            queue="myqueue",
            exchange="myexchange",
            routing_key="myqueue",
        )

    def test_exchange_not_declared_when_not_provided(self):
        from scrapy_distributed.amqp_utils.connection import get_channel

        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_declare = MagicMock()
        mock_channel.queue_declare.return_value = mock_declare

        get_channel(connection=mock_connection, queue="myqueue")

        mock_channel.exchange_declare.assert_not_called()
        mock_channel.queue_bind.assert_not_called()
