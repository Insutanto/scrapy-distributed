#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from twisted.internet.threads import deferToThread

logger = logging.getLogger(__name__)


class SqlAlchemyPipeline(object):
    """Scrapy pipeline that persists items to a relational database via SQLAlchemy.

    Configuration (in Scrapy settings):
        SQLALCHEMY_CONNECTION_STRING (str, required):
            A SQLAlchemy database URL, e.g.
            ``"sqlite:///items.db"`` or
            ``"postgresql://user:pass@localhost/mydb"``.
        SQLALCHEMY_TABLE_NAME (str, optional):
            Name of the table to write items into.
            Defaults to ``"<spider_name>_items"``.

    The table is created automatically when the spider opens.  All item
    fields are stored as ``Text`` columns.  If the table already contains
    a column not present in the current item the extra column is silently
    ignored; if the item contains a field that is absent from the table a
    new ``Text`` column is added via ``ALTER TABLE``.
    """

    def __init__(self, connection_string: str, table_name: str = None):
        self.connection_string = connection_string
        self._table_name = table_name
        self.engine = None
        self.table = None

    # ------------------------------------------------------------------
    # Class helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_crawler(cls, crawler):
        connection_string = crawler.settings.get("SQLALCHEMY_CONNECTION_STRING")
        if not connection_string:
            raise ValueError(
                "SQLALCHEMY_CONNECTION_STRING must be set to use SqlAlchemyPipeline"
            )
        table_name = crawler.settings.get("SQLALCHEMY_TABLE_NAME", None)
        return cls(
            connection_string=connection_string,
            table_name=table_name,
        )

    @classmethod
    def _default_table_name(cls, spider):
        return f"{spider.name}_items"

    # ------------------------------------------------------------------
    # Scrapy lifecycle
    # ------------------------------------------------------------------

    def open_spider(self, spider):
        from sqlalchemy import create_engine, MetaData, Table, Column, Text, inspect

        table_name = self._table_name or self._default_table_name(spider)
        logger.info(
            f"SqlAlchemyPipeline connecting to {self.connection_string!r}, "
            f"table={table_name!r}"
        )
        self.engine = create_engine(self.connection_string)
        self.metadata = MetaData()
        self._Text = Text
        self._Table = Table
        self._Column = Column
        self._inspect = inspect
        self.table_name = table_name

        # Reflect existing schema (if any) so we can extend it later.
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            self.table = Table(table_name, self.metadata, autoload_with=self.engine)
        else:
            self.table = None

    def close_spider(self, spider):
        if self.engine:
            self.engine.dispose()
            logger.info("SqlAlchemyPipeline engine disposed")

    # ------------------------------------------------------------------
    # Item processing
    # ------------------------------------------------------------------

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        row = dict(item)
        self._ensure_table(row)
        with self.engine.begin() as conn:
            conn.execute(self.table.insert(), row)
        return item

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_table(self, row: dict):
        """Create or extend the table to match *row*'s keys."""
        from sqlalchemy import Table, Column, Text, MetaData, text

        if self.table is None:
            # First item: create the table with the observed columns.
            columns = [Column(key, Text) for key in row.keys()]
            self.table = Table(self.table_name, self.metadata, *columns)
            self.metadata.create_all(self.engine)
        else:
            # Check for new columns and add them via ALTER TABLE.
            existing = {col.name for col in self.table.columns}
            new_keys = [k for k in row.keys() if k not in existing]
            if new_keys:
                preparer = self.engine.dialect.identifier_preparer
                quoted_table = preparer.quote(self.table_name)
                with self.engine.begin() as conn:
                    for key in new_keys:
                        quoted_col = preparer.quote(key)
                        conn.execute(
                            text(
                                f"ALTER TABLE {quoted_table}"
                                f" ADD COLUMN {quoted_col} TEXT"
                            )
                        )
                # Refresh the reflected table object.
                self.metadata = MetaData()
                self.table = Table(
                    self.table_name, self.metadata, autoload_with=self.engine
                )
