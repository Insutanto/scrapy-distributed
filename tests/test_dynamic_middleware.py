from unittest.mock import MagicMock

from scrapy.http import Request, TextResponse

from scrapy_distributed.middlewares.dynamic import DynamicCrawlerMiddleware


def _make_settings(**kwargs):
    defaults = {}
    defaults.update(kwargs)
    settings = MagicMock()
    settings.get = lambda k, default=None: defaults.get(k, default)
    return settings


def _make_spider():
    spider = MagicMock()
    spider.logger = MagicMock()
    return spider


class TestDynamicCrawlerMiddleware:
    def test_sets_user_agent_and_proxy(self):
        mw = DynamicCrawlerMiddleware(
            _make_settings(
                DYNAMIC_CRAWLER_USER_AGENTS=["ua-1"],
                DYNAMIC_CRAWLER_PROXIES=["http://127.0.0.1:9000"],
            )
        )
        request = Request("http://example.com")

        mw.process_request(request, _make_spider())

        assert request.headers.get("User-Agent") == b"ua-1"
        assert request.meta["proxy"] == "http://127.0.0.1:9000"

    def test_sets_playwright_click_methods(self):
        mw = DynamicCrawlerMiddleware(_make_settings())
        request = Request("http://example.com", meta={"dynamic_click_selectors": ["#load-more"]})

        class FakePageMethod:
            def __init__(self, method, selector):
                self.method = method
                self.selector = selector

        from scrapy_distributed.middlewares import dynamic as dynamic_module

        original = dynamic_module.PageMethod
        dynamic_module.PageMethod = FakePageMethod
        try:
            mw.process_request(request, _make_spider())
        finally:
            dynamic_module.PageMethod = original

        assert request.meta["playwright"] is True
        assert len(request.meta["playwright_page_methods"]) == 1
        assert request.meta["playwright_page_methods"][0].method == "click"
        assert request.meta["playwright_page_methods"][0].selector == "#load-more"

    def test_retries_blocked_response(self):
        mw = DynamicCrawlerMiddleware(_make_settings(DYNAMIC_CRAWLER_BLOCK_STATUSES=[403], DYNAMIC_CRAWLER_MAX_RETRY_TIMES=2))
        request = Request("http://example.com")
        response = TextResponse(url="http://example.com", status=403, body=b"blocked")

        result = mw.process_response(request, response, _make_spider())

        assert isinstance(result, Request)
        assert result.dont_filter is True
        assert result.meta["dynamic_retry_times"] == 1

    def test_stops_retry_after_limit(self):
        mw = DynamicCrawlerMiddleware(_make_settings(DYNAMIC_CRAWLER_BLOCK_STATUSES=[403], DYNAMIC_CRAWLER_MAX_RETRY_TIMES=1))
        request = Request("http://example.com", meta={"dynamic_retry_times": 1})
        response = TextResponse(url="http://example.com", status=403, body=b"blocked")

        result = mw.process_response(request, response, _make_spider())

        assert result is response
