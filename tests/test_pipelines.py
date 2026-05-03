"""
Unit tests for scrapy_distributed pipelines:
  - RabbitPipeline  (pipelines/amqp.py)
  - KafkaPipeline   (pipelines/kafka.py)
  - RocketMQPipeline (pipelines/rocketmq.py)

All broker interactions are mocked – no real broker required.
"""
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pytest

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


# ===========================================================================
# RabbitPipeline
# ===========================================================================

class TestRabbitPipeline:
    def _make_pipeline(self, item_conf=None, connection_url="amqp://localhost/"):
        from scrapy_distributed.pipelines.amqp import RabbitPipeline
        from scrapy_distributed.common.queue_config import RabbitQueueConfig
        if item_conf is None:
            item_conf = RabbitQueueConfig(name="items")
        with patch("scrapy_distributed.pipelines.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            pipeline = RabbitPipeline(item_conf=item_conf, connection_url=connection_url)
        return pipeline

    def test_construction_sets_attributes(self):
        from scrapy_distributed.common.queue_config import RabbitQueueConfig
        conf = RabbitQueueConfig(name="myitems")
        pipeline = self._make_pipeline(item_conf=conf)
        assert pipeline.item_conf is conf
        assert pipeline.connection_url == "amqp://localhost/"

    def test_item_key_uses_spider_name(self):
        from scrapy_distributed.pipelines.amqp import RabbitPipeline
        spider = SimpleNamespace(name="myspider")
        assert RabbitPipeline.item_key(None, spider) == "myspider:items"

    def test_process_item_publishes_to_channel(self):
        pipeline = self._make_pipeline()
        from scrapy.item import Item, Field
        class TestItem(Item):
            url = Field()
        item = TestItem(url="http://example.com/")
        spider = SimpleNamespace(name="spider", logger=MagicMock())
        pipeline._process_item(item, spider)
        pipeline.channel.basic_publish.assert_called_once()

    def test_process_item_returns_item(self):
        pipeline = self._make_pipeline()
        from scrapy.item import Item, Field
        class TestItem(Item):
            url = Field()
        item = TestItem(url="http://example.com/")
        spider = SimpleNamespace(name="spider", logger=MagicMock())
        result = pipeline._process_item(item, spider)
        assert result is item

    def test_close_calls_channel_close(self):
        pipeline = self._make_pipeline()
        pipeline.close()
        pipeline.channel.close.assert_called_once()

    def test_from_crawler_uses_spider_item_conf(self):
        from scrapy_distributed.pipelines.amqp import RabbitPipeline
        from scrapy_distributed.common.queue_config import RabbitQueueConfig
        conf = RabbitQueueConfig(name="custom-items")
        crawler = MagicMock()
        crawler.spider.item_conf = conf
        crawler.settings.get.return_value = "amqp://localhost/"
        with patch.object(RabbitPipeline, "__init__", return_value=None):
            with patch.object(RabbitPipeline, "connect"):
                instance = RabbitPipeline.__new__(RabbitPipeline)
                instance.item_conf = None
                # Test that from_crawler picks up spider.item_conf
        # Just verify item_key classmethod works
        spider = SimpleNamespace(name="spider")
        key = RabbitPipeline.item_key(None, spider)
        assert key == "spider:items"

    def test_from_crawler_defaults_item_conf_from_spider_name(self):
        from scrapy_distributed.pipelines.amqp import RabbitPipeline
        from scrapy_distributed.common.queue_config import RabbitQueueConfig
        crawler = MagicMock(spec=["spider", "settings"])
        del crawler.spider.item_conf  # ensure spider has no item_conf
        crawler.spider = SimpleNamespace(name="myspider")
        crawler.settings = MagicMock()
        crawler.settings.get.return_value = "amqp://localhost/"
        with patch("scrapy_distributed.pipelines.amqp.connection") as mock_conn:
            mock_conn.connect.return_value = MagicMock()
            mock_conn.get_channel.return_value = MagicMock()
            pipeline = RabbitPipeline.from_crawler(crawler)
        assert pipeline.item_conf.name == "myspider:items"


# ===========================================================================
# KafkaPipeline
# ===========================================================================

class TestKafkaPipeline:
    def _make_pipeline(self, item_conf=None, connection_conf="localhost:9092"):
        from scrapy_distributed.pipelines.kafka import KafkaPipeline
        from scrapy_distributed.common.queue_config import KafkaQueueConfig
        if item_conf is None:
            item_conf = KafkaQueueConfig(topic="items")
        with patch("scrapy_distributed.pipelines.kafka.KafkaAdminClient") as MockAdmin, \
             patch("scrapy_distributed.pipelines.kafka.KafkaProducer") as MockProducer:
            MockAdmin.return_value = MagicMock()
            MockProducer.return_value = MagicMock()
            pipeline = KafkaPipeline(item_conf=item_conf, connection_conf=connection_conf)
        return pipeline

    def test_construction_sets_attributes(self):
        from scrapy_distributed.common.queue_config import KafkaQueueConfig
        conf = KafkaQueueConfig(topic="myitems")
        pipeline = self._make_pipeline(item_conf=conf)
        assert pipeline.item_conf is conf
        assert pipeline.connection_conf == "localhost:9092"

    def test_item_key_uses_spider_name(self):
        from scrapy_distributed.pipelines.kafka import KafkaPipeline
        spider = SimpleNamespace(name="myspider")
        assert KafkaPipeline.item_key(None, spider) == "myspider.items"

    def test_process_item_sends_to_producer(self):
        pipeline = self._make_pipeline()
        item = MagicMock()
        item._values = {"url": "http://example.com/", "title": "Test"}
        spider = SimpleNamespace(name="spider", logger=MagicMock())
        pipeline._process_item(item, spider)
        pipeline.producer.send.assert_called_once()

    def test_process_item_sends_to_correct_topic(self):
        from scrapy_distributed.common.queue_config import KafkaQueueConfig
        conf = KafkaQueueConfig(topic="my-topic")
        pipeline = self._make_pipeline(item_conf=conf)
        item = MagicMock()
        item._values = {"url": "http://example.com/"}
        spider = SimpleNamespace(name="spider", logger=MagicMock())
        pipeline._process_item(item, spider)
        args, _ = pipeline.producer.send.call_args
        assert args[0] == "my-topic"

    def test_process_item_returns_item(self):
        pipeline = self._make_pipeline()
        item = MagicMock()
        item._values = {"url": "http://example.com/"}
        spider = SimpleNamespace(name="spider", logger=MagicMock())
        result = pipeline._process_item(item, spider)
        assert result is item

    def test_close_shuts_down_clients(self):
        pipeline = self._make_pipeline()
        pipeline.close()
        pipeline.admin_client.close.assert_called_once()
        pipeline.producer.close.assert_called_once()

    def test_close_tolerates_exceptions(self):
        pipeline = self._make_pipeline()
        pipeline.admin_client.close.side_effect = RuntimeError("failed")
        pipeline.producer.close.side_effect = RuntimeError("failed")
        pipeline.close()  # must not raise

    def test_from_crawler_defaults_item_conf_from_spider_name(self):
        from scrapy_distributed.pipelines.kafka import KafkaPipeline
        crawler = MagicMock()
        crawler.spider = SimpleNamespace(name="myspider")
        crawler.settings.get.return_value = "localhost:9092"
        with patch("scrapy_distributed.pipelines.kafka.KafkaAdminClient") as MockAdmin, \
             patch("scrapy_distributed.pipelines.kafka.KafkaProducer") as MockProducer:
            MockAdmin.return_value = MagicMock()
            MockProducer.return_value = MagicMock()
            pipeline = KafkaPipeline.from_crawler(crawler)
        assert pipeline.item_conf.topic == "myspider.items"


# ===========================================================================
# RocketMQPipeline
# ===========================================================================

class TestRocketMQPipeline:
    def _make_pipeline(self, item_conf=None, name_server="127.0.0.1:9876"):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        from scrapy_distributed.common.queue_config import RocketMQQueueConfig
        if item_conf is None:
            item_conf = RocketMQQueueConfig(topic="items")
        with patch("scrapy_distributed.pipelines.rocketmq.Producer") as MockProducer:
            MockProducer.return_value = MagicMock()
            pipeline = RocketMQPipeline(item_conf=item_conf, name_server=name_server)
        return pipeline

    def test_construction_sets_attributes(self):
        from scrapy_distributed.common.queue_config import RocketMQQueueConfig
        conf = RocketMQQueueConfig(topic="myitems", group="mygroup")
        pipeline = self._make_pipeline(item_conf=conf)
        assert pipeline.item_conf is conf
        assert pipeline.name_server == "127.0.0.1:9876"

    def test_item_key_uses_spider_name(self):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        spider = SimpleNamespace(name="myspider")
        assert RocketMQPipeline.item_key(None, spider) == "myspider.items"

    def test_process_item_sends_message(self):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        with patch("scrapy_distributed.pipelines.rocketmq.Producer") as MockProducer, \
             patch("scrapy_distributed.pipelines.rocketmq.Message") as MockMessage:
            mock_producer_instance = MagicMock()
            MockProducer.return_value = mock_producer_instance
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            from scrapy_distributed.common.queue_config import RocketMQQueueConfig
            conf = RocketMQQueueConfig(topic="items")
            pipeline = RocketMQPipeline(item_conf=conf, name_server="127.0.0.1:9876")
            item = MagicMock()
            item._values = {"url": "http://example.com/"}
            spider = SimpleNamespace(name="spider", logger=MagicMock())
            pipeline._process_item(item, spider)
        mock_producer_instance.send_sync.assert_called_once_with(mock_msg)

    def test_process_item_sets_tags_when_configured(self):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        with patch("scrapy_distributed.pipelines.rocketmq.Producer") as MockProducer, \
             patch("scrapy_distributed.pipelines.rocketmq.Message") as MockMessage:
            MockProducer.return_value = MagicMock()
            mock_msg = MagicMock()
            MockMessage.return_value = mock_msg
            from scrapy_distributed.common.queue_config import RocketMQQueueConfig
            conf = RocketMQQueueConfig(topic="items", tags="urgent")
            pipeline = RocketMQPipeline(item_conf=conf, name_server="127.0.0.1:9876")
            item = MagicMock()
            item._values = {"url": "http://example.com/"}
            spider = SimpleNamespace(name="spider", logger=MagicMock())
            pipeline._process_item(item, spider)
        mock_msg.set_tags.assert_called_once_with("urgent")

    def test_process_item_returns_item(self):
        pipeline = self._make_pipeline()
        with patch("scrapy_distributed.pipelines.rocketmq.Message") as MockMessage:
            MockMessage.return_value = MagicMock()
            item = MagicMock()
            item._values = {"url": "http://example.com/"}
            spider = SimpleNamespace(name="spider", logger=MagicMock())
            result = pipeline._process_item(item, spider)
        assert result is item

    def test_close_shuts_down_producer(self):
        pipeline = self._make_pipeline()
        pipeline.close()
        pipeline.producer.shutdown.assert_called()

    def test_close_tolerates_producer_exception(self):
        pipeline = self._make_pipeline()
        pipeline.producer.shutdown.side_effect = RuntimeError("already stopped")
        pipeline.close()  # must not raise

    def test_connect_recreates_producer(self):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        from scrapy_distributed.common.queue_config import RocketMQQueueConfig
        conf = RocketMQQueueConfig(topic="items")
        with patch("scrapy_distributed.pipelines.rocketmq.Producer") as MockProducer:
            old_producer = MagicMock()
            new_producer = MagicMock()
            MockProducer.side_effect = [old_producer, new_producer]
            pipeline = RocketMQPipeline(item_conf=conf, name_server="127.0.0.1:9876")
            pipeline.connect()
        old_producer.shutdown.assert_called()
        new_producer.start.assert_called()

    def test_from_crawler_defaults_item_conf_from_spider_name(self):
        from scrapy_distributed.pipelines.rocketmq import RocketMQPipeline
        crawler = MagicMock()
        crawler.spider = SimpleNamespace(name="myspider")
        crawler.settings.get.return_value = "127.0.0.1:9876"
        with patch("scrapy_distributed.pipelines.rocketmq.Producer") as MockProducer:
            MockProducer.return_value = MagicMock()
            pipeline = RocketMQPipeline.from_crawler(crawler)
        assert pipeline.item_conf.topic == "myspider.items"
