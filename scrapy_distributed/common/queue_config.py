#!/usr/bin/env python
# -*- coding: utf-8 -*-
class RabbitQueueConfig(object):
    def __init__(
        self,
        name,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        arguments=None,
        properties=None,
        exchange=None,
        exchange_type="direct",
        exchange_durable=True,
        exchange_arguments=None,
    ):
        self.name = name
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.properties = properties
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.exchange_durable = exchange_durable
        self.exchange_arguments = exchange_arguments


class KafkaQueueConfig(object):
    def __init__(
        self,
        topic,
        num_partitions=10,
        replication_factor=3,
        arguments=None,
    ):
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.arguments = arguments


class RocketMQQueueConfig(object):
    def __init__(self, topic, group="default", tags=None, keys=None, arguments=None):
        self.topic = topic
        self.group = group
        self.tags = tags
        self.keys = keys
        self.arguments = arguments