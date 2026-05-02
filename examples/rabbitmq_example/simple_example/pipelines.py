# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request
from simple_example.items import CommonExampleItem


class SimpleExamplePipeline:
    def process_item(self, item, spider):
        base_dir = spider.data_dir
        if isinstance(item, CommonExampleItem):
            with open(base_dir + "/" + item["title"] + ".html", "w+", encoding="utf-8") as f:
                f.write(item["content"])
        return item


class ImagePipeline(ImagesPipeline):

    def get_media_requests(self, item, info):
        for index, image_url in enumerate(item.get('image_urls', [])):
            yield Request(image_url, meta={'item': item, 'index': index})

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths and item.get('image_urls'):
            raise DropItem("Item contains no images")
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        meta_item = request.meta.get('item', {})
        image_guid = request.url.split('/')[-1]
        filename = './{}/{}/{}'.format(
            meta_item.get("url", "unknown").replace("/", "_"),
            meta_item.get('title', 'unknown'),
            image_guid
        )
        return filename


class MyFilesPipeline(FilesPipeline):

    def get_media_requests(self, item, info):
        for index, file_url in enumerate(item.get('file_urls', [])):
            yield Request(file_url, meta={'item': item, 'index': index})

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths and item.get('file_urls'):
            raise DropItem("Item contains no files")
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        meta_item = request.meta.get('item', {})
        file_guid = request.url.split('/')[-1]
        filename = './{}/{}/{}'.format(
            meta_item.get("url", "unknown").replace("/", "_"),
            meta_item.get('title', 'unknown'),
            file_guid
        )
        return filename
