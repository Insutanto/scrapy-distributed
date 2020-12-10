#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from kafka.admin.client import KafkaAdminClient
from kafka.admin.new_topic import NewTopic
from kafka.producer.kafka import KafkaProducer
from scrapy import spiders
from scrapy_distributed.common.queue_config import KafkaQueueConfig
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet.threads import deferToThread

default_serialize = ScrapyJSONEncoder().encode

logger = logging.getLogger(__name__)


class KafkaPipeline(object):
    def __init__(self, item_conf: KafkaQueueConfig, connection_conf: str):
        self.item_conf: KafkaQueueConfig = item_conf
        self.serialize = default_serialize
        self.connection_conf = connection_conf
        self.admin_client = None
        self.producer = None
        self.connect()
    
    @classmethod
    def from_crawler(cls, crawler):
        if hasattr(crawler.spider, "item_conf"):
            item_conf = crawler.spider.item_conf
        else:
            item_conf = KafkaQueueConfig(cls.item_key(None, crawler.spider))
        return cls(
            item_conf=item_conf,
            connection_conf=crawler.settings.get("KAFKA_CONNECTION_PARAMETERS"),
        )

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        body = self.serialize(item._values)
        self.producer.send(self.item_conf.topic, body.encode())
        spider.logger.info(f"produce: {body}")
        return item

    @classmethod
    def item_key(cls, item, spider):
        return f"{spider.name}.items"

    def connect(self):
        """Make a connection"""
        logger.info(f"connect kafka: {self.connection_conf}")
        if self.admin_client:
            try:
                self.admin_client.close()
            except:
                pass
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.connection_conf)
        topic_list = []
        topic_list.append(
            NewTopic(name=self.item_conf.topic, 
                    num_partitions=self.item_conf.num_partitions, 
                    replication_factor=self.item_conf.replication_factor))
        try:
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except Exception as e:
            logger.error(e)

        if self.producer:
            self.producer.close()
        self.producer = KafkaProducer(bootstrap_servers=self.connection_conf)


    def close(self):
        """Close channel"""
        self.admin_client.close()
        self.producer.close()
        self.consumer.close()
        logger.error("kafka pipeline channel is closed")
