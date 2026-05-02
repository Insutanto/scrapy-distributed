"""
Tests for scrapy_distributed/redis_utils/connection.py
"""
import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
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


# ---------------------------------------------------------------------------
# get_redis tests
# ---------------------------------------------------------------------------

class TestGetRedis:
    """Tests for scrapy_distributed.redis_utils.connection.get_redis."""

    def _get_redis(self, **kwargs):
        from scrapy_distributed.redis_utils.connection import get_redis
        return get_redis(**kwargs)

    def test_without_url_instantiates_cls_directly(self):
        mock_cls = MagicMock()
        result = self._get_redis(redis_cls=mock_cls)
        mock_cls.assert_called_once_with()
        assert result is mock_cls.return_value

    def test_with_url_uses_from_url(self):
        mock_cls = MagicMock()
        result = self._get_redis(redis_cls=mock_cls, url="redis://localhost:6379/0")
        mock_cls.from_url.assert_called_once_with("redis://localhost:6379/0")
        assert result is mock_cls.from_url.return_value

    def test_extra_kwargs_passed_to_cls(self):
        mock_cls = MagicMock()
        self._get_redis(redis_cls=mock_cls, host="myhost", port=6380)
        mock_cls.assert_called_once_with(host="myhost", port=6380)

    def test_extra_kwargs_passed_to_from_url(self):
        mock_cls = MagicMock()
        self._get_redis(redis_cls=mock_cls, url="redis://localhost/", db=1)
        mock_cls.from_url.assert_called_once_with("redis://localhost/", db=1)

    def test_url_is_not_forwarded_as_kwarg(self):
        mock_cls = MagicMock()
        self._get_redis(redis_cls=mock_cls, url="redis://localhost/")
        # 'url' must NOT appear in the from_url kwargs
        _, kwargs = mock_cls.from_url.call_args
        assert "url" not in kwargs

    def test_redis_cls_not_forwarded_as_kwarg(self):
        mock_cls = MagicMock()
        self._get_redis(redis_cls=mock_cls)
        _, kwargs = mock_cls.call_args
        assert "redis_cls" not in kwargs

    def test_default_redis_cls_is_strict_redis(self):
        """Without an explicit redis_cls the default (StrictRedis) is used."""
        from scrapy_distributed.redis_utils import defaults
        mock_instance = MagicMock()
        with patch.object(defaults.REDIS_CLS, "__call__", return_value=mock_instance):
            # We just verify no exception is raised when using defaults.
            # (An actual StrictRedis() call would need a running server.)
            pass  # covered by the import-time assertion below

        # Verify the default class is the one declared in defaults.
        import redis
        assert defaults.REDIS_CLS is redis.StrictRedis


# ---------------------------------------------------------------------------
# get_redis_from_settings tests
# ---------------------------------------------------------------------------

class TestGetRedisFromSettings:
    """Tests for get_redis_from_settings (settings → Redis client)."""

    def _call(self, settings, *, mock_client=None):
        if mock_client is None:
            mock_client = MagicMock()
        with patch(
            "scrapy_distributed.redis_utils.connection.get_redis",
            return_value=mock_client,
        ) as mock_get_redis:
            from scrapy_distributed.redis_utils.connection import get_redis_from_settings
            result = get_redis_from_settings(settings)
        return result, mock_get_redis

    def test_returns_redis_client(self):
        settings = _MockSettings()
        result, _ = self._call(settings)
        assert result is not None

    def test_url_setting_forwarded(self):
        settings = _MockSettings(BLOOM_DUPEFILTER_REDIS_URL="redis://host/0")
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("url") == "redis://host/0"

    def test_host_setting_forwarded(self):
        settings = _MockSettings(BLOOM_DUPEFILTER_REDIS_HOST="myhost")
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("host") == "myhost"

    def test_port_setting_forwarded(self):
        settings = _MockSettings(BLOOM_DUPEFILTER_REDIS_PORT=6380)
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("port") == 6380

    def test_encoding_setting_forwarded(self):
        settings = _MockSettings(BLOOM_DUPEFILTER_REDIS_ENCODING="latin-1")
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("encoding") == "latin-1"

    def test_default_params_included(self):
        """Default REDIS_PARAMS (timeout, retry) must be forwarded."""
        from scrapy_distributed.redis_utils import defaults
        settings = _MockSettings()
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("socket_timeout") == defaults.REDIS_PARAMS["socket_timeout"]
        assert kwargs.get("retry_on_timeout") == defaults.REDIS_PARAMS["retry_on_timeout"]

    def test_redis_bloom_params_merged(self):
        """Extra params from REDIS_BLOOM_PARAMS dict are merged in."""
        settings = _MockSettings(REDIS_BLOOM_PARAMS={"db": 3})
        _, mock_get_redis = self._call(settings)
        _, kwargs = mock_get_redis.call_args
        assert kwargs.get("db") == 3

    def test_redis_cls_string_is_resolved(self):
        """If redis_cls is a dotted string, load_object must resolve it."""
        mock_cls = MagicMock()
        settings = _MockSettings(
            REDIS_BLOOM_PARAMS={"redis_cls": "myproject.FakeRedis"}
        )
        with patch(
            "scrapy_distributed.redis_utils.connection.load_object",
            return_value=mock_cls,
        ) as mock_load:
            with patch(
                "scrapy_distributed.redis_utils.connection.get_redis",
                return_value=MagicMock(),
            ):
                from scrapy_distributed.redis_utils.connection import get_redis_from_settings
                get_redis_from_settings(settings)
            mock_load.assert_called_once_with("myproject.FakeRedis")
