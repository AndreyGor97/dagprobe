"""Shim ``...operators.clickhouse_dbapi``: BaseDbApi/SQL/BranchSQL операторы плагина."""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class ClickHouseBaseDbApiOperator(BaseOperator):
    """Стаб базового DB-API оператора плагина (наследники — BranchSQL и пр.)."""

    template_fields = ("sql", "parameters", "database")

    def __init__(
        self,
        *,
        sql: Any = None,
        parameters: dict[str, Any] | None = None,
        database: str | None = None,
        clickhouse_conn_id: str = "clickhouse_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.parameters = parameters or {}
        self.database = database
        self.clickhouse_conn_id = clickhouse_conn_id


class ClickHouseSQLOperator(ClickHouseBaseDbApiOperator):
    """Стаб ``ClickHouseSQLOperator``."""


class ClickHouseBranchSQLOperator(ClickHouseBaseDbApiOperator):
    """Стаб ``ClickHouseBranchSQLOperator``: ветки разбирает движок прогона."""
