import pytest
from scrapy_distributed.common.queue_config import (
    RabbitQueueConfig,
    KafkaQueueConfig,
    RocketMQQueueConfig,
)


class TestRabbitQueueConfig:
    def test_required_name(self):
        cfg = RabbitQueueConfig(name="myqueue")
        assert cfg.name == "myqueue"

    def test_defaults(self):
        cfg = RabbitQueueConfig(name="q")
        assert cfg.passive is False
        assert cfg.durable is False
        assert cfg.exclusive is False
        assert cfg.auto_delete is False
        assert cfg.arguments is None
        assert cfg.properties is None

    def test_custom_values(self):
        cfg = RabbitQueueConfig(
            name="tasks",
            passive=True,
            durable=True,
            exclusive=True,
            auto_delete=True,
            arguments={"x-max-priority": 10},
            properties={"delivery_mode": 2},
        )
        assert cfg.name == "tasks"
        assert cfg.passive is True
        assert cfg.durable is True
        assert cfg.exclusive is True
        assert cfg.auto_delete is True
        assert cfg.arguments == {"x-max-priority": 10}
        assert cfg.properties == {"delivery_mode": 2}


class TestKafkaQueueConfig:
    def test_required_topic(self):
        cfg = KafkaQueueConfig(topic="events")
        assert cfg.topic == "events"

    def test_defaults(self):
        cfg = KafkaQueueConfig(topic="t")
        assert cfg.num_partitions == 10
        assert cfg.replication_factor == 3
        assert cfg.arguments is None

    def test_custom_values(self):
        cfg = KafkaQueueConfig(
            topic="crawl",
            num_partitions=4,
            replication_factor=1,
            arguments={"compression.type": "gzip"},
        )
        assert cfg.topic == "crawl"
        assert cfg.num_partitions == 4
        assert cfg.replication_factor == 1
        assert cfg.arguments == {"compression.type": "gzip"}


class TestRocketMQQueueConfig:
    def test_required_topic(self):
        cfg = RocketMQQueueConfig(topic="orders")
        assert cfg.topic == "orders"

    def test_defaults(self):
        cfg = RocketMQQueueConfig(topic="t")
        assert cfg.group == "default"
        assert cfg.tags is None
        assert cfg.keys is None
        assert cfg.arguments is None

    def test_custom_values(self):
        cfg = RocketMQQueueConfig(
            topic="pay",
            group="pay_group",
            tags="urgent",
            keys="order_id",
            arguments={"delay_level": 1},
        )
        assert cfg.topic == "pay"
        assert cfg.group == "pay_group"
        assert cfg.tags == "urgent"
        assert cfg.keys == "order_id"
        assert cfg.arguments == {"delay_level": 1}
