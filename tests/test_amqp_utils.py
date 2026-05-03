"""
Unit tests for scrapy_distributed.amqp_utils.connection.

All pika interactions are mocked – no real broker required.
"""
import pytest
from unittest.mock import MagicMock, patch, call


# ===========================================================================
# get_channel
# ===========================================================================

class TestGetChannel:
    def _call(self, queue="test-queue", **kwargs):
        from scrapy_distributed.amqp_utils.connection import get_channel
        connection = MagicMock()
        mock_channel = MagicMock()
        connection.channel.return_value = mock_channel
        result = get_channel(connection, queue, **kwargs)
        return result, connection, mock_channel

    def test_returns_channel_object(self):
        result, _, _ = self._call()
        assert result is not None

    def test_opens_channel_from_connection(self):
        result, connection, mock_channel = self._call()
        connection.channel.assert_called_once()
        assert result is mock_channel

    def test_declares_queue_with_queue_name(self):
        result, _, mock_channel = self._call(queue="my-queue")
        mock_channel.queue_declare.assert_called_once()
        args, kwargs = mock_channel.queue_declare.call_args
        assert kwargs.get("queue") == "my-queue"

    def test_passes_passive_flag(self):
        _, _, ch = self._call(passive=True)
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["passive"] is True

    def test_passes_durable_flag(self):
        _, _, ch = self._call(durable=True)
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["durable"] is True

    def test_passes_exclusive_flag(self):
        _, _, ch = self._call(exclusive=True)
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["exclusive"] is True

    def test_passes_auto_delete_flag(self):
        _, _, ch = self._call(auto_delete=True)
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["auto_delete"] is True

    def test_passes_arguments(self):
        _, _, ch = self._call(arguments={"x-max-priority": 10})
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["arguments"] == {"x-max-priority": 10}

    def test_default_flags_are_false(self):
        _, _, ch = self._call()
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["passive"] is False
        assert kwargs["durable"] is False
        assert kwargs["exclusive"] is False
        assert kwargs["auto_delete"] is False

    def test_default_arguments_is_none(self):
        _, _, ch = self._call()
        _, kwargs = ch.queue_declare.call_args
        assert kwargs["arguments"] is None


# ===========================================================================
# connect
# ===========================================================================

class TestConnect:
    def test_connect_returns_blocking_connection(self):
        from scrapy_distributed.amqp_utils.connection import connect
        mock_connection = MagicMock()
        with patch("scrapy_distributed.amqp_utils.connection.BlockingConnection",
                   return_value=mock_connection) as MockBC:
            result = connect("amqp://guest:guest@localhost/")
        assert result is mock_connection
        MockBC.assert_called_once()

    def test_connect_passes_url_parameters(self):
        from scrapy_distributed.amqp_utils.connection import connect
        with patch("scrapy_distributed.amqp_utils.connection.URLParameters") as MockURLParams, \
             patch("scrapy_distributed.amqp_utils.connection.BlockingConnection") as MockBC:
            MockBC.return_value = MagicMock()
            connect("amqp://user:pass@host:5672/vhost")
        MockURLParams.assert_called_once_with("amqp://user:pass@host:5672/vhost")

    def test_connect_uses_url_parameters_in_blocking_connection(self):
        from scrapy_distributed.amqp_utils.connection import connect
        mock_url_params = MagicMock()
        with patch("scrapy_distributed.amqp_utils.connection.URLParameters",
                   return_value=mock_url_params), \
             patch("scrapy_distributed.amqp_utils.connection.BlockingConnection") as MockBC:
            MockBC.return_value = MagicMock()
            connect("amqp://localhost/")
        MockBC.assert_called_once_with(mock_url_params)
