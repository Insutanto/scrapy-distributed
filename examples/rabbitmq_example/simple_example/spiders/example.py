from scrapy.http.request import Request
from scrapy.selector.unified import Selector
from scrapy.spiders import Spider
from examples.rabbitmq_example.simple_example.items import CommonExampleItem, SimpleExampleItem
from scrapy_distributed.spiders.sitemap import SitemapSpider
from scrapy_distributed.common.queue_config import RabbitQueueConfig
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig


class RabbitSitemapSpider(SitemapSpider):
    name = "example"
    sitemap_urls = ["http://www.people.com.cn/robots.txt"]
    queue_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
        properties={"delivery_mode": 2}
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


class RabbitCommonSpider(Spider):
    name = "example_common"
    start_urls = ["http://www.people.com.cn/"]
    allowed_domains = ["people.com.cn"]
    queue_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example.rabbit.common",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
        properties={"delivery_mode": 2}
    )
    item_conf: RabbitQueueConfig = RabbitQueueConfig(
        name="example.rabbit.common.items",
        durable=True,
        arguments={"x-queue-mode": "lazy", "x-max-priority": 255},
    )
    redis_bloom_conf: RedisBloomConfig = RedisBloomConfig(
        key="example.rabbit.common.items", error_rate=0.001, capacity=100_0000,
        exclude_url_query_params=False
    )
    data_dir = "./test_data/example_common"


    def parse(self, response):
        self.logger.info(f"parse response, url: {response.url}")
        for link in response.xpath("//a/@href").extract():
            if not link.startswith('http'): 
                    link = response.url + link
            yield Request(url=link)
        item = CommonExampleItem()
        item['url'] = response.url
        item['title'] = response.xpath("//title/text()").extract_first()
        item["content"] = response.text
        yield item
