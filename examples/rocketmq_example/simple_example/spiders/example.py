from examples.rocketmq_example.simple_example.items import SimpleExampleItem
from scrapy_distributed.common.queue_config import RocketMQQueueConfig
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig
from scrapy_distributed.spiders.sitemap import SitemapSpider


class RocketMQSpider(SitemapSpider):
    name = "example3"
    sitemap_urls = ["http://www.people.com.cn/robots.txt"]
    queue_conf: RocketMQQueueConfig = RocketMQQueueConfig(
        topic="example3",
        group="example3",
    )
    redis_bloom_conf: RedisBloomConfig = RedisBloomConfig(
        key="example3:dupefilter", error_rate=0.001, capacity=100_0000
    )
    item_conf: RocketMQQueueConfig = RocketMQQueueConfig(
        topic="example3.items",
        group="example3.items",
    )

    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
        item = SimpleExampleItem()
        item["url"] = response.url
        yield item
