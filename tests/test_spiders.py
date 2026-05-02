"""
Tests for:
  - scrapy_distributed/spiders/redis_bloom.py  (RedisBloomMixin)
  - scrapy_distributed/spiders/sitemap.py       (SitemapSpider)
"""
import gzip
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scrapy.http import Request, XmlResponse, Response, TextResponse


# ===========================================================================
# RedisBloomMixin
# ===========================================================================

from scrapy_distributed.spiders.redis_bloom import RedisBloomMixin
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig
from scrapy_distributed.redis_utils import defaults as redis_defaults


class _DummySpider(RedisBloomMixin):
    """Minimal spider-like class that mixes in RedisBloomMixin."""
    name = "dummy"

    def __init__(self):
        # Don't call super().__init__() – no real Scrapy init needed.
        self.redis_client = None
        self.bloom_key = None


class TestRedisBloomMixinInit:
    def test_default_attributes(self):
        mixin = _DummySpider()
        assert mixin.redis_client is None
        assert mixin.bloom_key is None

    def test_constructor_accepts_redis_client_and_bloom_key(self):
        rc = MagicMock()
        mixin = RedisBloomMixin(redis_client=rc, bloom_key="mykey")
        assert mixin.redis_client is rc
        assert mixin.bloom_key == "mykey"


class TestRedisBloomMixinRequestSuccess:
    """Tests for RedisBloomMixin.request_success."""

    def _make_mixin(self, bloom_key="test:bf"):
        mixin = _DummySpider()
        mixin.redis_client = MagicMock()
        mixin.bloom_key = bloom_key
        return mixin

    def test_calls_bfAdd_with_netloc_and_path(self):
        mixin = self._make_mixin()
        mixin.request_success("http://example.com/some/page")
        mixin.redis_client.bfAdd.assert_called_once_with(
            "test:bf", "example.com/some/page"
        )

    def test_strips_scheme_from_uri(self):
        mixin = self._make_mixin()
        mixin.request_success("https://example.com/path")
        uri = mixin.redis_client.bfAdd.call_args[0][1]
        assert not uri.startswith("http")
        assert uri == "example.com/path"

    def test_strips_query_string(self):
        mixin = self._make_mixin()
        mixin.request_success("http://example.com/page?foo=bar")
        uri = mixin.redis_client.bfAdd.call_args[0][1]
        assert "foo" not in uri
        assert uri == "example.com/page"

    def test_uses_correct_bloom_key(self):
        mixin = self._make_mixin(bloom_key="spider:custom:bf")
        mixin.request_success("http://example.com/")
        key_used = mixin.redis_client.bfAdd.call_args[0][0]
        assert key_used == "spider:custom:bf"

    def test_returns_bfAdd_result(self):
        mixin = self._make_mixin()
        mixin.redis_client.bfAdd.return_value = 1
        result = mixin.request_success("http://example.com/")
        assert result == 1


class TestRedisBloomMixinSetupRedisClient:
    """Tests for RedisBloomMixin.setup_redis_client."""

    def _make_crawler(self, **settings_data):
        class _Settings:
            def get(self, key, default=None):
                return settings_data.get(key, default)
            def getdict(self, key, default=None):
                return settings_data.get(key, default or {})

        crawler = MagicMock()
        crawler.settings = _Settings()
        return crawler

    def test_sets_redis_client_from_settings(self):
        mixin = _DummySpider()
        mock_client = MagicMock()
        crawler = self._make_crawler()
        with patch(
            "scrapy_distributed.spiders.redis_bloom.get_redis_from_settings",
            return_value=mock_client,
        ):
            mixin.setup_redis_client(crawler)
        assert mixin.redis_client is mock_client

    def test_uses_redis_bloom_conf_key_when_present(self):
        mixin = _DummySpider()
        mixin.redis_bloom_conf = RedisBloomConfig(key="custom:bf:key")
        crawler = self._make_crawler()
        with patch(
            "scrapy_distributed.spiders.redis_bloom.get_redis_from_settings",
            return_value=MagicMock(),
        ):
            mixin.setup_redis_client(crawler)
        assert mixin.bloom_key == "custom:bf:key"

    def test_uses_default_key_format_when_no_conf(self):
        mixin = _DummySpider()  # no redis_bloom_conf
        crawler = self._make_crawler()
        with patch(
            "scrapy_distributed.spiders.redis_bloom.get_redis_from_settings",
            return_value=MagicMock(),
        ):
            mixin.setup_redis_client(crawler)
        expected = redis_defaults.SCHEDULER_DUPEFILTER_KEY % {"spider": "dummy"}
        assert mixin.bloom_key == expected

    def test_custom_key_template_in_settings(self):
        mixin = _DummySpider()
        crawler = self._make_crawler(
            REDIS_BLOOM_DUPEFILTER_KEY="%(spider)s:custom:dupefilter"
        )
        with patch(
            "scrapy_distributed.spiders.redis_bloom.get_redis_from_settings",
            return_value=MagicMock(),
        ):
            mixin.setup_redis_client(crawler)
        assert mixin.bloom_key == "dummy:custom:dupefilter"


# ===========================================================================
# SitemapSpider
# ===========================================================================

from scrapy_distributed.spiders.sitemap import SitemapSpider


# ---------------------------------------------------------------------------
# A concrete subclass that can be instantiated without a full Scrapy crawler.
# ---------------------------------------------------------------------------

class _SitemapSpider(SitemapSpider):
    name = "test_sitemap"
    sitemap_urls = ["http://example.com/sitemap.xml"]


_URLSET_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/page1</loc></url>
  <url><loc>http://example.com/page2</loc></url>
</urlset>
"""

_SITEMAPINDEX_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://example.com/sitemap1.xml</loc></sitemap>
  <sitemap><loc>http://example.com/sitemap2.xml</loc></sitemap>
</sitemapindex>
"""

_ROBOTS_TXT = b"""\
User-agent: *
Disallow: /private/

Sitemap: http://example.com/sitemap.xml
"""


def _xml_response(url, body):
    return XmlResponse(url=url, body=body)


def _plain_response(url, body, headers=None):
    return TextResponse(url=url, body=body, headers=headers or {})


class TestSitemapSpiderStartRequests:
    def test_yields_request_for_each_sitemap_url(self):
        spider = _SitemapSpider()
        requests = list(spider.start_requests())
        assert len(requests) == 1
        assert requests[0].url == "http://example.com/sitemap.xml"

    def test_no_sitemap_urls_yields_nothing(self):
        class NoUrlSpider(SitemapSpider):
            name = "empty"
            sitemap_urls = []

        spider = NoUrlSpider()
        assert list(spider.start_requests()) == []

    def test_multiple_sitemap_urls(self):
        class MultiSpider(SitemapSpider):
            name = "multi"
            sitemap_urls = [
                "http://example.com/sitemap1.xml",
                "http://example.com/sitemap2.xml",
            ]

        spider = MultiSpider()
        requests = list(spider.start_requests())
        urls = [r.url for r in requests]
        assert "http://example.com/sitemap1.xml" in urls
        assert "http://example.com/sitemap2.xml" in urls


class TestSitemapSpiderSitemapFilter:
    def test_default_filter_passes_all_entries(self):
        spider = _SitemapSpider()
        entries = [{"loc": "http://example.com/a"}, {"loc": "http://example.com/b"}]
        result = list(spider.sitemap_filter(entries))
        assert result == entries

    def test_empty_entries(self):
        spider = _SitemapSpider()
        assert list(spider.sitemap_filter([])) == []

    def test_custom_filter_can_exclude_entries(self):
        class FilteredSpider(SitemapSpider):
            name = "filtered"
            sitemap_urls = []

            def sitemap_filter(self, entries):
                for e in entries:
                    if "include" in e.get("loc", ""):
                        yield e

        spider = FilteredSpider()
        entries = [
            {"loc": "http://example.com/include/a"},
            {"loc": "http://example.com/skip/b"},
        ]
        result = list(spider.sitemap_filter(entries))
        assert len(result) == 1
        assert result[0]["loc"] == "http://example.com/include/a"


class TestSitemapSpiderGetSitemapBody:
    def test_xml_response_returns_body_directly(self):
        spider = _SitemapSpider()
        response = _xml_response("http://example.com/sitemap.xml", _URLSET_XML)
        body = spider._get_sitemap_body(response)
        assert body == _URLSET_XML

    def test_gzipped_body_is_decompressed(self):
        spider = _SitemapSpider()
        compressed = gzip.compress(_URLSET_XML)
        response = _plain_response("http://example.com/sitemap.xml.gz", compressed)
        body = spider._get_sitemap_body(response)
        assert body == _URLSET_XML

    def test_xml_url_with_plain_body(self):
        spider = _SitemapSpider()
        response = _plain_response("http://example.com/sitemap.xml", _URLSET_XML)
        body = spider._get_sitemap_body(response)
        assert body == _URLSET_XML

    def test_xml_gz_url_with_plain_body(self):
        spider = _SitemapSpider()
        response = _plain_response("http://example.com/sitemap.xml.gz", _URLSET_XML)
        body = spider._get_sitemap_body(response)
        assert body == _URLSET_XML

    def test_unknown_response_returns_none(self):
        spider = _SitemapSpider()
        response = _plain_response("http://example.com/notasitemap.html", b"<html/>")
        body = spider._get_sitemap_body(response)
        assert body is None


class TestSitemapSpiderParseSitemap:
    def _spider(self):
        return _SitemapSpider()

    def test_urlset_yields_requests_for_each_loc(self):
        spider = self._spider()
        response = _xml_response("http://example.com/sitemap.xml", _URLSET_XML)
        results = list(spider._parse_sitemap(response))
        urls = [r.url for r in results if isinstance(r, Request)]
        assert "http://example.com/page1" in urls
        assert "http://example.com/page2" in urls

    def test_sitemapindex_yields_requests_for_sub_sitemaps(self):
        spider = self._spider()
        response = _xml_response("http://example.com/sitemap-index.xml", _SITEMAPINDEX_XML)
        results = list(spider._parse_sitemap(response))
        urls = [r.url for r in results if isinstance(r, Request)]
        assert "http://example.com/sitemap1.xml" in urls
        assert "http://example.com/sitemap2.xml" in urls

    def test_sitemapindex_sub_requests_have_parse_sitemap_callback(self):
        spider = self._spider()
        response = _xml_response("http://example.com/sitemap-index.xml", _SITEMAPINDEX_XML)
        results = list(spider._parse_sitemap(response))
        for req in results:
            assert isinstance(req, Request)
            # Bound-method identity differs on each access; compare __func__ instead.
            assert req.callback.__func__ is _SitemapSpider._parse_sitemap

    def test_invalid_body_logs_warning_and_returns_nothing(self):
        spider = self._spider()
        # A plain HTML response with a non-sitemap URL returns None body.
        response = _plain_response("http://example.com/not-a-sitemap.html", b"<html/>")
        results = list(spider._parse_sitemap(response))
        assert results == []

    def test_robots_txt_yields_sitemap_requests(self):
        spider = self._spider()
        response = _plain_response("http://example.com/robots.txt", _ROBOTS_TXT)
        results = list(spider._parse_sitemap(response))
        urls = [r.url for r in results if isinstance(r, Request)]
        assert "http://example.com/sitemap.xml" in urls

    def test_sitemap_rules_filter_urlset_locs(self):
        """Only locs matching at least one rule callback are yielded."""
        class RuledSpider(SitemapSpider):
            name = "ruled"
            sitemap_urls = []
            sitemap_rules = [("/products/", "parse")]

        spider = RuledSpider()
        xml = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/products/widget</loc></url>
  <url><loc>http://example.com/about</loc></url>
</urlset>
"""
        response = _xml_response("http://example.com/sitemap.xml", xml)
        results = list(spider._parse_sitemap(response))
        urls = [r.url for r in results if isinstance(r, Request)]
        assert "http://example.com/products/widget" in urls
        assert "http://example.com/about" not in urls

    def test_sitemap_follow_filters_sitemapindex_locs(self):
        """Only sub-sitemaps whose loc matches sitemap_follow are followed."""
        class FollowSpider(SitemapSpider):
            name = "follow"
            sitemap_urls = []
            sitemap_follow = ["/news/"]

        spider = FollowSpider()
        xml = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://example.com/news/sitemap.xml</loc></sitemap>
  <sitemap><loc>http://example.com/products/sitemap.xml</loc></sitemap>
</sitemapindex>
"""
        response = _xml_response("http://example.com/sitemap-index.xml", xml)
        results = list(spider._parse_sitemap(response))
        urls = [r.url for r in results if isinstance(r, Request)]
        assert "http://example.com/news/sitemap.xml" in urls
        assert "http://example.com/products/sitemap.xml" not in urls
