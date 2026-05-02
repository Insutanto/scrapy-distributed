import importlib.util
import os
import sys
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from scrapy_distributed.dupefilters.base import BaseDupeFilter
from scrapy_distributed.dupefilters.redis_bloom import RedisBloomConfig, RedisBloomDupeFilter
from scrapy_distributed.redis_utils import defaults


# ---------------------------------------------------------------------------
# Load scheduler mixins without triggering the broken schedulers/__init__.py
# (scrapy_distributed.queues.kafka still imports scrapy.utils.reqser which
#  was removed in Scrapy 2.6+, so importing the package __init__.py fails.)
# ---------------------------------------------------------------------------

def _load_scheduler_module(module_name, filename):
    """Load a file under scrapy_distributed/schedulers/ in isolation."""
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "scrapy_distributed",
            "schedulers",
            filename,
        )
    )
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod  # register before exec so cross-imports resolve
    spec.loader.exec_module(mod)
    return mod


_dupefilter_mod = _load_scheduler_module(
    "scrapy_distributed.schedulers.common_dupefilter",
    "common_dupefilter.py",
)
DupeFilterSchedulerMixin = _dupefilter_mod.DupeFilterSchedulerMixin

_redis_bloom_scheduler_mod = _load_scheduler_module(
    "scrapy_distributed.schedulers.redis_bloom",
    "redis_bloom.py",
)
RedisBloomSchedulerMixin = _redis_bloom_scheduler_mod.RedisBloomSchedulerMixin


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

    def test_init_dupefilter_raises_when_class_not_configured(self):
        """A missing DUPEFILTER_CLASS setting should raise KeyError."""
        mixin = DupeFilterSchedulerMixin()
        spider = SpiderWithConf()
        spider.settings = DummySettings()  # no DUPEFILTER_CLASS key
        with pytest.raises(KeyError, match="DUPEFILTER_CLASS"):
            mixin.init_dupefilter(spider)


# ---------------------------------------------------------------------------
# RedisBloomConfig tests
# ---------------------------------------------------------------------------

class TestRedisBloomConfig:
    def test_defaults(self):
        cfg = RedisBloomConfig(key="mykey")
        assert cfg.key == "mykey"
        assert cfg.error_rate == 0.001
        assert cfg.capacity == 100_0000
        assert cfg.exclude_url_query_params is None
        assert cfg.kwargs == {}

    def test_custom_values(self):
        cfg = RedisBloomConfig(
            key="k",
            error_rate=0.01,
            capacity=50000,
            exclude_url_query_params=False,
            kwargs={"a": 1},
        )
        assert cfg.error_rate == 0.01
        assert cfg.capacity == 50000
        assert cfg.exclude_url_query_params is False
        assert cfg.kwargs == {"a": 1}

    def test_kwargs_none_becomes_empty_dict(self):
        cfg = RedisBloomConfig(key="k", kwargs=None)
        assert cfg.kwargs == {}


# ---------------------------------------------------------------------------
# Helpers shared by RedisBloomDupeFilter tests
# ---------------------------------------------------------------------------

class _MockSettings:
    """Minimal Scrapy-Settings-compatible stub."""

    def __init__(self, **kwargs):
        self._data = kwargs

    def get(self, key, default=None):
        return self._data.get(key, default)

    def getbool(self, key, default=False):
        return bool(self._data.get(key, default))

    def getdict(self, key, default=None):
        return self._data.get(key, default or {})


def _make_filter(config=None, exclude_url_query_params=True):
    """Return a RedisBloomDupeFilter with a mock Redis client."""
    redis_client = MagicMock()
    flt = RedisBloomDupeFilter(
        redis_client=redis_client,
        config=config,
        default_exclude_url_query_params=exclude_url_query_params,
    )
    return flt, redis_client


# ---------------------------------------------------------------------------
# RedisBloomDupeFilter tests
# ---------------------------------------------------------------------------

class TestRedisBloomDupeFilter:

    # --- __init__ ---

    def test_init_sets_defaults(self):
        flt, rc = _make_filter()
        assert flt.redis_client is rc
        assert flt.config is None
        assert flt.dupe_filter_key == defaults.SCHEDULER_DUPEFILTER_KEY
        assert flt.default_error_rate == defaults.DUPEFILTER_ERROR_RATE
        assert flt.default_capacity == defaults.DUPEFILTER_CAPACITY
        assert flt.logdupes is True
        assert flt.debug is False

    # --- request_seen ---

    def test_request_seen_new_url_returns_false_and_adds(self):
        cfg = RedisBloomConfig(key="test:bf")
        flt, rc = _make_filter(config=cfg)
        rc.bfExists.return_value = 0  # not yet in bloom filter

        req = SimpleNamespace(url="http://example.com/page")
        assert flt.request_seen(req) is False
        rc.bfAdd.assert_called_once()

    def test_request_seen_existing_url_returns_true_no_add(self):
        cfg = RedisBloomConfig(key="test:bf")
        flt, rc = _make_filter(config=cfg)
        rc.bfExists.return_value = 1  # already in bloom filter

        req = SimpleNamespace(url="http://example.com/page")
        assert flt.request_seen(req) is True
        rc.bfAdd.assert_not_called()

    def test_request_seen_strips_query_string_by_default(self):
        """When exclude_url_query_params is not False, query is stripped."""
        cfg = RedisBloomConfig(key="test:bf", exclude_url_query_params=None)
        flt, rc = _make_filter(config=cfg, exclude_url_query_params=True)
        rc.bfExists.return_value = 0

        req = SimpleNamespace(url="http://example.com/page?foo=bar")
        flt.request_seen(req)
        # The URI passed to bfExists/bfAdd must not contain the query string
        uri_used = rc.bfExists.call_args[0][1]
        assert "foo" not in uri_used
        assert uri_used == "example.com/page"

    def test_request_seen_keeps_query_when_config_exclude_false(self):
        """When config.exclude_url_query_params is False, query is kept."""
        cfg = RedisBloomConfig(key="test:bf", exclude_url_query_params=False)
        flt, rc = _make_filter(config=cfg, exclude_url_query_params=True)
        rc.bfExists.return_value = 0

        req = SimpleNamespace(url="http://example.com/page?foo=bar")
        flt.request_seen(req)
        uri_used = rc.bfExists.call_args[0][1]
        assert "foo=bar" in uri_used

    def test_request_seen_keeps_query_when_default_exclude_false(self):
        """When default_exclude_url_query_params is False, query is kept."""
        cfg = RedisBloomConfig(key="test:bf", exclude_url_query_params=None)
        flt, rc = _make_filter(config=cfg, exclude_url_query_params=False)
        rc.bfExists.return_value = 0

        req = SimpleNamespace(url="http://example.com/page?foo=bar")
        flt.request_seen(req)
        uri_used = rc.bfExists.call_args[0][1]
        assert "foo=bar" in uri_used

    # --- open / close / clear ---

    def test_open_with_none_spider_is_noop(self):
        flt, rc = _make_filter()
        result = flt.open(spider=None)
        assert result is None
        rc.bfCreate.assert_not_called()

    def test_open_with_spider_initialises_bloom_key(self):
        cfg = RedisBloomConfig(key="s:bf")
        flt, rc = _make_filter(config=cfg)
        rc.exists.return_value = 0

        spider = SimpleNamespace(name="myspider")
        flt.open(spider)
        rc.bfCreate.assert_called_once_with(cfg.key, cfg.error_rate, cfg.capacity)

    def test_close_is_noop(self):
        flt, rc = _make_filter()
        flt.close("finished")  # must not raise

    def test_clear_deletes_key(self):
        cfg = RedisBloomConfig(key="s:bf")
        flt, rc = _make_filter(config=cfg)
        flt.clear()
        rc.delete.assert_called_once_with(cfg.key)

    # --- init_redis_bloom_key ---

    def test_init_redis_bloom_key_uses_spider_conf(self):
        """Spider's redis_bloom_conf overrides default key."""
        cfg = RedisBloomConfig(key="spider:custom:bf")
        flt, rc = _make_filter()  # no config set
        rc.exists.return_value = 1  # key already exists → no bfCreate

        spider = SimpleNamespace(name="sp", redis_bloom_conf=cfg)
        flt.init_redis_bloom_key(spider)
        assert flt.config is cfg

    def test_init_redis_bloom_key_uses_default_key_format(self):
        """Without redis_bloom_conf, uses dupe_filter_key % spider.name."""
        flt, rc = _make_filter()
        rc.exists.return_value = 0

        spider = SimpleNamespace(name="myspider")
        flt.init_redis_bloom_key(spider)
        expected_key = defaults.SCHEDULER_DUPEFILTER_KEY % {"spider": "myspider"}
        assert flt.config.key == expected_key

    def test_init_redis_bloom_key_creates_if_not_exists(self):
        cfg = RedisBloomConfig(key="s:bf")
        flt, rc = _make_filter(config=cfg)
        rc.exists.return_value = 0  # key absent

        spider = SimpleNamespace(name="sp")
        flt.init_redis_bloom_key(spider)
        rc.bfCreate.assert_called_once_with(cfg.key, cfg.error_rate, cfg.capacity)

    def test_init_redis_bloom_key_skips_create_if_exists(self):
        cfg = RedisBloomConfig(key="s:bf")
        flt, rc = _make_filter(config=cfg)
        rc.exists.return_value = 1  # key present

        spider = SimpleNamespace(name="sp")
        flt.init_redis_bloom_key(spider)
        rc.bfCreate.assert_not_called()

    # --- from_settings ---

    def test_from_settings_reads_settings_values(self):
        settings = _MockSettings(
            DUPEFILTER_DEBUG=True,
            SCHEDULER_DUPEFILTER_KEY="%(spider)s:custom",
            BLOOM_DUPEFILTER_ERROR_RATE=0.005,
            BLOOM_DUPEFILTER_CAPACITY=200_000,
            BLOOM_DUPEFILTER_EXCLUDE_URL_QUERY_PARAMS=False,
        )
        mock_client = MagicMock()
        with patch(
            "scrapy_distributed.dupefilters.redis_bloom.get_redis_from_settings",
            return_value=mock_client,
        ):
            flt = RedisBloomDupeFilter.from_settings(settings)

        assert flt.debug is True
        assert flt.dupe_filter_key == "%(spider)s:custom"
        assert flt.default_error_rate == 0.005
        assert flt.default_capacity == 200_000
        assert flt.default_exclude_url_query_params is False
        assert flt.redis_client is mock_client

    def test_from_settings_uses_defaults_when_not_set(self):
        settings = _MockSettings()
        mock_client = MagicMock()
        with patch(
            "scrapy_distributed.dupefilters.redis_bloom.get_redis_from_settings",
            return_value=mock_client,
        ):
            flt = RedisBloomDupeFilter.from_settings(settings)

        assert flt.default_error_rate == defaults.DUPEFILTER_ERROR_RATE
        assert flt.default_capacity == defaults.DUPEFILTER_CAPACITY
        assert flt.dupe_filter_key == defaults.SCHEDULER_DUPEFILTER_KEY


# ---------------------------------------------------------------------------
# RedisBloomSchedulerMixin tests
# ---------------------------------------------------------------------------

class TestRedisBloomSchedulerMixin:
    def test_init_redis_bloom_delegates_to_init_dupefilter(self):
        """init_redis_bloom is a backward-compat alias for init_dupefilter."""
        mixin = RedisBloomSchedulerMixin()
        spider = SpiderWithConf()
        mixin.init_redis_bloom(spider)
        # The mixin should have set self.df via init_dupefilter
        assert type(mixin.df).__name__ == "ConcreteFilter"

    def test_redis_bloom_mixin_is_subclass_of_dupefilter_mixin(self):
        assert issubclass(RedisBloomSchedulerMixin, DupeFilterSchedulerMixin)

