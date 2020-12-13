# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
from examples.rabbitmq_example.simple_example.items import CommonExampleItem
from itemadapter import ItemAdapter
import json


class SimpleExamplePipeline:
    def process_item(self, item, spider):
        base_dir = spider.data_dir
        if isinstance(item, CommonExampleItem):
            with open(base_dir + "/" + item["title"] + ".html", "w+", encoding="utf-8") as f:
                f.write(item["content"])
        return item
