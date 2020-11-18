from pyasn1.type.base import SimpleAsn1Type
from scrapy_distributed.queues.kafka import KafkaQueueConfig
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

    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
