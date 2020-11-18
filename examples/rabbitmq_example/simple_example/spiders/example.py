from scrapy_distributed.spiders.sitemap import SitemapSpider
from scrapy_distributed.queues.amqp import RabbitQueueConfig
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig


class RabbitSpider(SitemapSpider):
    name = "example"
    sitemap_urls = ["http://www.people.com.cn/robots.txt"]
    queue_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
    )
    redis_bloom_conf: RedisBloomConfig = RedisBloomConfig(
        key="example:dupefilter", error_rate=0.001, capacity=100_0000
    )

    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
