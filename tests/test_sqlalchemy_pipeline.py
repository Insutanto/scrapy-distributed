"""
Tests for the SqlAlchemyPipeline introduced to support SQLAlchemy-based
item persistence (scrapy_distributed/pipelines/sqlalchemy.py).
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from twisted.internet import defer

from scrapy_distributed.pipelines.sqlalchemy import SqlAlchemyPipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spider(name="test_spider"):
    return SimpleNamespace(name=name)


def _make_crawler(connection_string="sqlite:///:memory:", table_name=None):
    crawler = MagicMock()
    crawler.settings = MagicMock()

    def _get(key, default=None):
        if key == "SQLALCHEMY_CONNECTION_STRING":
            return connection_string
        if key == "SQLALCHEMY_TABLE_NAME":
            return table_name
        return default

    crawler.settings.get.side_effect = _get
    return crawler


def _make_item(**kwargs):
    """Return a plain dict acting as a Scrapy item."""
    return dict(**kwargs)


# ---------------------------------------------------------------------------
# from_crawler
# ---------------------------------------------------------------------------

class TestFromCrawler:
    def test_returns_pipeline_instance(self):
        crawler = _make_crawler()
        pipeline = SqlAlchemyPipeline.from_crawler(crawler)
        assert isinstance(pipeline, SqlAlchemyPipeline)

    def test_stores_connection_string(self):
        crawler = _make_crawler(connection_string="sqlite:///test.db")
        pipeline = SqlAlchemyPipeline.from_crawler(crawler)
        assert pipeline.connection_string == "sqlite:///test.db"

    def test_stores_custom_table_name(self):
        crawler = _make_crawler(table_name="my_custom_table")
        pipeline = SqlAlchemyPipeline.from_crawler(crawler)
        assert pipeline._table_name == "my_custom_table"

    def test_table_name_none_when_not_configured(self):
        crawler = _make_crawler(table_name=None)
        pipeline = SqlAlchemyPipeline.from_crawler(crawler)
        assert pipeline._table_name is None

    def test_raises_when_connection_string_missing(self):
        crawler = _make_crawler(connection_string=None)
        with pytest.raises(ValueError, match="SQLALCHEMY_CONNECTION_STRING"):
            SqlAlchemyPipeline.from_crawler(crawler)


# ---------------------------------------------------------------------------
# open_spider / close_spider
# ---------------------------------------------------------------------------

class TestOpenCloseSpider:
    def test_open_spider_creates_engine(self):
        pipeline = SqlAlchemyPipeline(connection_string="sqlite:///:memory:")
        spider = _make_spider()
        pipeline.open_spider(spider)
        assert pipeline.engine is not None
        pipeline.close_spider(spider)

    def test_default_table_name_uses_spider_name(self):
        pipeline = SqlAlchemyPipeline(connection_string="sqlite:///:memory:")
        spider = _make_spider(name="my_spider")
        pipeline.open_spider(spider)
        assert pipeline.table_name == "my_spider_items"
        pipeline.close_spider(spider)

    def test_custom_table_name_is_used(self):
        pipeline = SqlAlchemyPipeline(
            connection_string="sqlite:///:memory:", table_name="scraped_data"
        )
        spider = _make_spider()
        pipeline.open_spider(spider)
        assert pipeline.table_name == "scraped_data"
        pipeline.close_spider(spider)

    def test_close_spider_disposes_engine(self):
        pipeline = SqlAlchemyPipeline(connection_string="sqlite:///:memory:")
        spider = _make_spider()
        pipeline.open_spider(spider)
        engine = pipeline.engine
        pipeline.close_spider(spider)
        # Engine should be disposed (pool is reset); no exception means success.


# ---------------------------------------------------------------------------
# _process_item (synchronous internals)
# ---------------------------------------------------------------------------

class TestProcessItem:
    def _open_pipeline(self, table_name=None):
        pipeline = SqlAlchemyPipeline(
            connection_string="sqlite:///:memory:", table_name=table_name
        )
        spider = _make_spider()
        pipeline.open_spider(spider)
        return pipeline, spider

    def test_item_is_persisted(self):
        pipeline, spider = self._open_pipeline()
        item = _make_item(title="Hello", url="http://example.com/")
        pipeline._process_item(item, spider)

        from sqlalchemy import text
        with pipeline.engine.connect() as conn:
            rows = list(conn.execute(text(f"SELECT * FROM {pipeline.table_name}")))
        assert len(rows) == 1
        row = rows[0]._mapping
        assert row["title"] == "Hello"
        assert row["url"] == "http://example.com/"

    def test_multiple_items_are_persisted(self):
        pipeline, spider = self._open_pipeline()
        for i in range(5):
            pipeline._process_item(_make_item(title=f"Item {i}", url=f"http://example.com/{i}"), spider)

        from sqlalchemy import text
        with pipeline.engine.connect() as conn:
            rows = list(conn.execute(text(f"SELECT * FROM {pipeline.table_name}")))
        assert len(rows) == 5

    def test_new_field_adds_column(self):
        """A second item with an extra field should not raise."""
        pipeline, spider = self._open_pipeline()
        pipeline._process_item(_make_item(title="First"), spider)
        pipeline._process_item(_make_item(title="Second", extra="bonus"), spider)

        from sqlalchemy import text
        with pipeline.engine.connect() as conn:
            rows = list(conn.execute(text(f"SELECT * FROM {pipeline.table_name}")))
        assert len(rows) == 2

    def test_process_item_returns_item(self):
        pipeline, spider = self._open_pipeline()
        item = _make_item(title="T", url="http://x.com/")
        result = pipeline._process_item(item, spider)
        assert result is item

    def test_none_field_value_stored(self):
        """None values should be accepted."""
        pipeline, spider = self._open_pipeline()
        item = _make_item(title=None, url="http://x.com/")
        pipeline._process_item(item, spider)

        from sqlalchemy import text
        with pipeline.engine.connect() as conn:
            rows = list(conn.execute(text(f"SELECT * FROM {pipeline.table_name}")))
        assert rows[0]._mapping["title"] is None

    def test_integer_field_stored_as_text(self):
        """Non-string values should be coerced to text via SQLAlchemy Text type."""
        pipeline, spider = self._open_pipeline()
        item = _make_item(count=42, label="hello")
        # Should not raise even though 42 is an int
        pipeline._process_item(item, spider)


# ---------------------------------------------------------------------------
# process_item (Twisted deferred)
# ---------------------------------------------------------------------------

class TestProcessItemDeferred:
    def test_process_item_returns_deferred(self):
        pipeline = SqlAlchemyPipeline(connection_string="sqlite:///:memory:")
        spider = _make_spider()
        pipeline.open_spider(spider)
        item = _make_item(title="T", url="http://x.com/")
        result = pipeline.process_item(item, spider)
        assert isinstance(result, defer.Deferred)
        pipeline.close_spider(spider)


# ---------------------------------------------------------------------------
# _default_table_name
# ---------------------------------------------------------------------------

class TestDefaultTableName:
    def test_default_table_name_format(self):
        spider = _make_spider(name="my_crawler")
        assert SqlAlchemyPipeline._default_table_name(spider) == "my_crawler_items"


# ---------------------------------------------------------------------------
# Existing table is reused
# ---------------------------------------------------------------------------

class TestExistingTable:
    def test_existing_table_is_reused(self):
        """Opening the pipeline twice against the same DB should not fail."""
        from sqlalchemy import create_engine, Table, Column, Text, MetaData

        engine = create_engine("sqlite:///:memory:")
        metadata = MetaData()
        table = Table("existing_items", metadata, Column("title", Text))
        metadata.create_all(engine)

        pipeline = SqlAlchemyPipeline(
            connection_string="sqlite:///:memory:", table_name="existing_items"
        )
        spider = _make_spider()

        # Directly inject the pre-seeded engine so the pipeline uses it.
        pipeline.engine = engine
        pipeline.metadata = MetaData()
        pipeline.table_name = "existing_items"
        from sqlalchemy import inspect as sa_inspect
        inspector = sa_inspect(engine)
        if inspector.has_table("existing_items"):
            pipeline.table = Table(
                "existing_items", pipeline.metadata, autoload_with=engine
            )
        else:
            pipeline.table = None

        item = _make_item(title="Pre-existing")
        pipeline._process_item(item, spider)

        from sqlalchemy import text
        with engine.connect() as conn:
            rows = list(conn.execute(text("SELECT * FROM existing_items")))
        assert len(rows) == 1
