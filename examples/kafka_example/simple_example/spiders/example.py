from examples.kafka_example.simple_example.items import SimpleExampleItem
from scrapy_distributed.common.queue_config import KafkaQueueConfig
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig
from scrapy_distributed.spiders.sitemap import SitemapSpider


class KafkaSpider(SitemapSpider):
    name = "example2"
    sitemap_urls = ["http://www.people.com.cn/robots.txt"]
    queue_conf: KafkaQueueConfig = KafkaQueueConfig(
        topic="example2",
        num_partitions=1,
        replication_factor=1
    )
    redis_bloom_conf: RedisBloomConfig = RedisBloomConfig(
        key="example2:dupefilter", error_rate=0.001, capacity=100_0000
    )
    item_conf: KafkaQueueConfig = KafkaQueueConfig(
        topic="example2.items",
        num_partitions=1,
        replication_factor=1
    )

    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
        item = SimpleExampleItem()
        item["url"] = response.url
        yield item
