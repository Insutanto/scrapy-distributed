import importlib.util
import os
import pytest
from types import SimpleNamespace
from unittest.mock import patch

from scrapy_distributed.dupefilters.base import BaseDupeFilter


# ---------------------------------------------------------------------------
# Load DupeFilterSchedulerMixin without triggering broken package __init__.py
# ---------------------------------------------------------------------------

def _load_common_dupefilter():
    path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "scrapy_distributed",
        "schedulers",
        "common_dupefilter.py",
    )
    spec = importlib.util.spec_from_file_location(
        "scrapy_distributed.schedulers.common_dupefilter", os.path.abspath(path)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_dupefilter_mod = _load_common_dupefilter()
DupeFilterSchedulerMixin = _dupefilter_mod.DupeFilterSchedulerMixin


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class DummySettings(dict):
    """Minimal settings stub with a .get() method."""
    def get(self, key, default=None):
        return super().get(key, default)


class ConcreteFilter(BaseDupeFilter):
    """Minimal concrete DupeFilter for testing."""

    def __init__(self):
        self.seen = set()
        self.opened_with = None

    @classmethod
    def from_settings(cls, settings):
        return cls()

    @classmethod
    def from_spider(cls, spider):
        instance = cls.from_settings(spider.settings)
        instance.opened_with = spider
        return instance

    def request_seen(self, request):
        fp = request.url
        if fp in self.seen:
            return True
        self.seen.add(fp)
        return False


class SpiderWithConf(SimpleNamespace):
    name = "test"
    settings = DummySettings(
        DUPEFILTER_CLASS="tests.test_dupefilter.ConcreteFilter"
    )


# ---------------------------------------------------------------------------
# BaseDupeFilter tests
# ---------------------------------------------------------------------------

class TestBaseDupeFilter:
    def test_from_spider_returns_instance(self):
        spider = SpiderWithConf()
        instance = ConcreteFilter.from_spider(spider)
        assert isinstance(instance, ConcreteFilter)

    def test_from_spider_passes_spider(self):
        spider = SpiderWithConf()
        instance = ConcreteFilter.from_spider(spider)
        assert instance.opened_with is spider

    def test_request_seen_deduplicates(self):
        instance = ConcreteFilter()
        req1 = SimpleNamespace(url="http://example.com/a")
        req2 = SimpleNamespace(url="http://example.com/b")

        assert not instance.request_seen(req1)
        assert instance.request_seen(req1)   # duplicate
        assert not instance.request_seen(req2)

    def test_base_from_spider_calls_from_settings(self):
        """The default BaseDupeFilter.from_spider delegates to from_settings."""

        class MinimalFilter(BaseDupeFilter):
            created_from_settings = False

            @classmethod
            def from_settings(cls, settings):
                inst = cls()
                inst.created_from_settings = True
                return inst

        spider = SpiderWithConf()
        inst = MinimalFilter.from_spider(spider)
        assert inst.created_from_settings

    def test_concrete_filter_is_subclass(self):
        from scrapy.dupefilters import BaseDupeFilter as ScrapyBase
        assert issubclass(ConcreteFilter, ScrapyBase)
        assert issubclass(ConcreteFilter, BaseDupeFilter)


# ---------------------------------------------------------------------------
# DupeFilterSchedulerMixin tests
# ---------------------------------------------------------------------------

class TestDupeFilterSchedulerMixin:
    def test_init_dupefilter_sets_df(self):
        mixin = DupeFilterSchedulerMixin()
        spider = SpiderWithConf()
        mixin.init_dupefilter(spider)
        assert type(mixin.df).__name__ == "ConcreteFilter"

    def test_init_dupefilter_calls_from_spider(self):
        mixin = DupeFilterSchedulerMixin()
        spider = SpiderWithConf()
        mixin.init_dupefilter(spider)
        # ConcreteFilter.from_spider stores the spider reference
        assert mixin.df.opened_with is spider

    def test_init_dupefilter_loads_class_from_settings(self):
        """init_dupefilter must honour DUPEFILTER_CLASS setting."""
        mixin = DupeFilterSchedulerMixin()

        class AltFilter(BaseDupeFilter):
            @classmethod
            def from_spider(cls, spider):
                return cls()

        spider = SpiderWithConf()
        # The path value doesn't need to be importable because load_object is
        # patched below; we only verify it is forwarded unchanged.
        dummy_path = "myproject.filters.AltFilter"
        spider.settings = DummySettings(DUPEFILTER_CLASS=dummy_path)
        with patch.object(_dupefilter_mod, "load_object", return_value=AltFilter) as mock_load:
            mixin.init_dupefilter(spider)
            mock_load.assert_called_once_with(dummy_path)
        assert isinstance(mixin.df, AltFilter)

