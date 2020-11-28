from examples.rabbitmq_example.simple_example.items import SimpleExampleItem
from scrapy_distributed.spiders.sitemap import SitemapSpider
from scrapy_distributed.common.queue_config import RabbitQueueConfig
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig


class RabbitSpider(SitemapSpider):
    name = "example"
    sitemap_urls = ["http://www.people.com.cn/robots.txt"]
    queue_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
    )
    item_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example:items:new",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
    )
    redis_bloom_conf: RedisBloomConfig = RedisBloomConfig(
        key="example:dupefilter", error_rate=0.001, capacity=100_0000,
        exclude_url_query_params=False
    )

    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
        item = SimpleExampleItem()
        item['url'] = response.url
        item['title'] = response.xpath("//title/text()").extract()[0]
        yield item
