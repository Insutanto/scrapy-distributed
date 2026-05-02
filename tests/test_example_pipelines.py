"""
Tests for the rabbitmq example pipelines, items, and spider parse logic
introduced in issue #7 (image / file download support).
"""

import os
import sys
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, mock_open

# ---------------------------------------------------------------------------
# Path setup – the example package is not installed; add it to sys.path.
# ---------------------------------------------------------------------------
_EXAMPLES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "examples", "rabbitmq_example")
)
if _EXAMPLES_DIR not in sys.path:
    sys.path.insert(0, _EXAMPLES_DIR)

import scrapy
from scrapy.http import HtmlResponse, Request as ScrapyRequest
from scrapy.exceptions import DropItem

from simple_example.items import CommonExampleItem, SimpleExampleItem
from simple_example.pipelines import ImagePipeline, MyFilesPipeline, SimpleExamplePipeline

# The spider module imports CommonExampleItem via the full dotted path
# (examples.rabbitmq_example.simple_example.items), while the pipelines/items
# modules imported above use the shorter path (simple_example.items).
# Both paths refer to the same source file but produce distinct class objects.
# Use the spider's version of the class so isinstance() checks are consistent.
from examples.rabbitmq_example.simple_example.items import (
    CommonExampleItem as _SpiderCommonItem,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spider(name="example_common", data_dir="/tmp/test_data"):
    """Return a minimal spider stub."""
    return SimpleNamespace(name=name, data_dir=data_dir)


def _html_response(url, body_html):
    """Return a Scrapy HtmlResponse from a byte string."""
    return HtmlResponse(url=url, body=body_html.encode("utf-8"))


def _make_crawler():
    """Return a minimal crawler mock suitable for Spider.from_crawler."""
    crawler = MagicMock()
    crawler.settings = MagicMock()
    crawler.settings.get.return_value = None
    crawler.settings.getbool.return_value = False
    return crawler


# ---------------------------------------------------------------------------
# CommonExampleItem
# ---------------------------------------------------------------------------

class TestCommonExampleItem:
    def test_all_fields_defined(self):
        item = CommonExampleItem()
        item["title"] = "My Title"
        item["url"] = "http://example.com/"
        item["content"] = "<html/>"
        item["image_urls"] = ["http://example.com/a.jpg"]
        item["images"] = [{"path": "a.jpg"}]
        item["file_urls"] = ["http://example.com/doc.pdf"]
        item["files"] = [{"path": "doc.pdf"}]
        assert item["title"] == "My Title"
        assert item["image_urls"] == ["http://example.com/a.jpg"]
        assert item["file_urls"] == ["http://example.com/doc.pdf"]

    def test_undefined_field_raises(self):
        item = CommonExampleItem()
        with pytest.raises(KeyError):
            item["nonexistent"] = "value"


# ---------------------------------------------------------------------------
# SimpleExamplePipeline
# ---------------------------------------------------------------------------

class TestSimpleExamplePipeline:
    def test_process_item_writes_html_for_common_item(self, tmp_path):
        pipeline = SimpleExamplePipeline()
        spider = _make_spider(data_dir=str(tmp_path))
        item = CommonExampleItem()
        item["title"] = "PageTitle"
        item["url"] = "http://example.com/"
        item["content"] = "<html>hello</html>"
        result = pipeline.process_item(item, spider)
        assert result is item
        written = (tmp_path / "PageTitle.html").read_text(encoding="utf-8")
        assert written == "<html>hello</html>"

    def test_process_item_skips_non_common_item(self, tmp_path):
        pipeline = SimpleExamplePipeline()
        spider = _make_spider(data_dir=str(tmp_path))
        item = SimpleExampleItem()
        item["title"] = "T"
        item["url"] = "http://example.com/"
        result = pipeline.process_item(item, spider)
        assert result is item
        # No file should be written
        assert list(tmp_path.iterdir()) == []


# ---------------------------------------------------------------------------
# ImagePipeline
# ---------------------------------------------------------------------------

class TestImagePipeline:
    """Tests for ImagePipeline (subclass of scrapy ImagesPipeline)."""

    # ----- get_media_requests -----

    def test_get_media_requests_yields_request_per_url(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = ["http://example.com/a.jpg", "http://example.com/b.png"]
        reqs = list(pipeline.get_media_requests(item, None))
        assert len(reqs) == 2
        assert reqs[0].url == "http://example.com/a.jpg"
        assert reqs[1].url == "http://example.com/b.png"

    def test_get_media_requests_sets_index_in_meta(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = ["http://example.com/a.jpg", "http://example.com/b.png"]
        reqs = list(pipeline.get_media_requests(item, None))
        assert reqs[0].meta["index"] == 0
        assert reqs[1].meta["index"] == 1

    def test_get_media_requests_stores_item_in_meta(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = ["http://example.com/a.jpg"]
        req = next(pipeline.get_media_requests(item, None))
        assert req.meta["item"] is item

    def test_get_media_requests_empty_list_yields_nothing(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = []
        reqs = list(pipeline.get_media_requests(item, None))
        assert reqs == []

    def test_get_media_requests_missing_field_yields_nothing(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()  # image_urls not set
        reqs = list(pipeline.get_media_requests(item, None))
        assert reqs == []

    # ----- item_completed -----

    def test_item_completed_returns_item_when_downloads_succeed(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = ["http://example.com/a.jpg"]
        results = [(True, {"path": "full/path/a.jpg"})]
        returned = pipeline.item_completed(results, item, None)
        assert returned is item

    def test_item_completed_raises_drop_when_urls_set_but_none_succeed(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = ["http://example.com/a.jpg"]
        results = [(False, Exception("failed"))]
        with pytest.raises(DropItem, match="no images"):
            pipeline.item_completed(results, item, None)

    def test_item_completed_passes_through_item_with_no_image_urls(self):
        """Items that legitimately have no images must not be dropped."""
        pipeline = ImagePipeline.__new__(ImagePipeline)
        item = CommonExampleItem()
        item["image_urls"] = []
        results = []
        returned = pipeline.item_completed(results, item, None)
        assert returned is item

    # ----- file_path -----

    def test_file_path_returns_expected_format(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        meta_item = {"url": "http://example.com/page", "title": "My Page"}
        req = ScrapyRequest(
            "http://example.com/img/photo.jpg",
            meta={"item": meta_item},
        )
        path = pipeline.file_path(req)
        assert path == "./http:__example.com_page/My Page/photo.jpg"

    def test_file_path_replaces_slashes_in_url(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        meta_item = {"url": "http://a.com/x/y/z", "title": "T"}
        req = ScrapyRequest("http://a.com/images/pic.jpg", meta={"item": meta_item})
        path = pipeline.file_path(req)
        assert "/" not in path.split("/", 1)[1].split("/")[0]  # url part has no slashes after prefix

    def test_file_path_uses_unknown_defaults_when_meta_empty(self):
        pipeline = ImagePipeline.__new__(ImagePipeline)
        req = ScrapyRequest("http://example.com/img/photo.jpg", meta={})
        path = pipeline.file_path(req)
        assert "unknown" in path


# ---------------------------------------------------------------------------
# MyFilesPipeline
# ---------------------------------------------------------------------------

class TestMyFilesPipeline:
    """Tests for MyFilesPipeline (subclass of scrapy FilesPipeline)."""

    # ----- get_media_requests -----

    def test_get_media_requests_yields_request_per_url(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = ["http://example.com/doc.pdf", "http://example.com/data.xlsx"]
        reqs = list(pipeline.get_media_requests(item, None))
        assert len(reqs) == 2
        assert reqs[0].url == "http://example.com/doc.pdf"
        assert reqs[1].url == "http://example.com/data.xlsx"

    def test_get_media_requests_sets_index_in_meta(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = ["http://example.com/a.pdf", "http://example.com/b.docx"]
        reqs = list(pipeline.get_media_requests(item, None))
        assert reqs[0].meta["index"] == 0
        assert reqs[1].meta["index"] == 1

    def test_get_media_requests_stores_item_in_meta(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = ["http://example.com/doc.pdf"]
        req = next(pipeline.get_media_requests(item, None))
        assert req.meta["item"] is item

    def test_get_media_requests_empty_list_yields_nothing(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = []
        assert list(pipeline.get_media_requests(item, None)) == []

    def test_get_media_requests_missing_field_yields_nothing(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()  # file_urls not set
        assert list(pipeline.get_media_requests(item, None)) == []

    # ----- item_completed -----

    def test_item_completed_returns_item_when_downloads_succeed(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = ["http://example.com/doc.pdf"]
        results = [(True, {"path": "full/path/doc.pdf"})]
        assert pipeline.item_completed(results, item, None) is item

    def test_item_completed_raises_drop_when_urls_set_but_none_succeed(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = ["http://example.com/doc.pdf"]
        results = [(False, Exception("failed"))]
        with pytest.raises(DropItem, match="no files"):
            pipeline.item_completed(results, item, None)

    def test_item_completed_passes_through_item_with_no_file_urls(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        item = CommonExampleItem()
        item["file_urls"] = []
        assert pipeline.item_completed([], item, None) is item

    # ----- file_path -----

    def test_file_path_returns_expected_format(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        meta_item = {"url": "http://example.com/page", "title": "My Page"}
        req = ScrapyRequest(
            "http://example.com/docs/report.pdf",
            meta={"item": meta_item},
        )
        path = pipeline.file_path(req)
        assert path == "./http:__example.com_page/My Page/report.pdf"

    def test_file_path_uses_unknown_defaults_when_meta_empty(self):
        pipeline = MyFilesPipeline.__new__(MyFilesPipeline)
        req = ScrapyRequest("http://example.com/docs/report.pdf", meta={})
        path = pipeline.file_path(req)
        assert "unknown" in path


# ---------------------------------------------------------------------------
# RabbitCommonSpider.parse
# ---------------------------------------------------------------------------

class TestRabbitCommonSpiderParse:
    """Unit tests for RabbitCommonSpider.parse (no network required)."""

    @pytest.fixture
    def spider(self):
        from simple_example.spiders.example import RabbitCommonSpider
        return RabbitCommonSpider.from_crawler(_make_crawler())

    def test_parse_yields_item_with_basic_fields(self, spider):
        resp = _html_response(
            "http://example.com/",
            "<html><head><title>Hello</title></head><body></body></html>",
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert len(items) == 1
        item = items[0]
        assert item["url"] == "http://example.com/"
        assert item["title"] == "Hello"
        assert "<html>" in item["content"]

    def test_parse_extracts_jpg_images(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/p"><img src="/img/photo.jpg"/></a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["image_urls"] == ["http://example.com/img/photo.jpg"]

    def test_parse_extracts_png_images(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/p"><img src="/img/banner.png"/></a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert "http://example.com/img/banner.png" in items[0]["image_urls"]

    def test_parse_skips_non_jpg_png_images(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/p"><img src="/img/animation.gif"/></a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["image_urls"] == []

    def test_parse_resolves_relative_image_urls(self, spider):
        resp = _html_response(
            "http://example.com/section/",
            '<html><head><title>T</title></head><body>'
            '<a href="/p"><img src="../img/photo.jpg"/></a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        url = items[0]["image_urls"][0]
        assert url.startswith("http://")
        assert "photo.jpg" in url

    def test_parse_keeps_absolute_image_urls(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/p"><img src="http://cdn.example.com/photo.jpg"/></a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["image_urls"] == ["http://cdn.example.com/photo.jpg"]

    def test_parse_extracts_pdf_file_urls(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/docs/report.pdf">Download</a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["file_urls"] == ["http://example.com/docs/report.pdf"]

    def test_parse_extracts_docx_xlsx_zip_file_urls(self, spider):
        body = (
            '<html><head><title>T</title></head><body>'
            '<a href="/a.docx">docx</a>'
            '<a href="/b.xlsx">xlsx</a>'
            '<a href="/c.zip">zip</a>'
            '</body></html>'
        )
        resp = _html_response("http://example.com/", body)
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        file_urls = items[0]["file_urls"]
        assert "http://example.com/a.docx" in file_urls
        assert "http://example.com/b.xlsx" in file_urls
        assert "http://example.com/c.zip" in file_urls

    def test_parse_skips_html_as_file_url(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="/page.html">page</a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["file_urls"] == []

    def test_parse_yields_requests_for_links(self, spider):
        resp = _html_response(
            "http://example.com/",
            '<html><head><title>T</title></head><body>'
            '<a href="http://example.com/page1">p1</a>'
            '<a href="/page2">p2</a>'
            '</body></html>',
        )
        results = list(spider.parse(resp))
        requests = [r for r in results if isinstance(r, ScrapyRequest)]
        request_urls = [r.url for r in requests]
        assert "http://example.com/page1" in request_urls

    def test_parse_empty_page(self, spider):
        resp = _html_response(
            "http://example.com/",
            "<html><head><title>Empty</title></head><body></body></html>",
        )
        results = list(spider.parse(resp))
        items = [r for r in results if isinstance(r, _SpiderCommonItem)]
        assert items[0]["image_urls"] == []
        assert items[0]["file_urls"] == []
