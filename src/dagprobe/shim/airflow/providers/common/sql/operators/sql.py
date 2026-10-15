"""Shim ``providers.common.sql.operators.sql``: SQLExecuteQueryOperator/BranchSQLOperator."""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class SQLExecuteQueryOperator(BaseOperator):
    """Стаб: sql/conn_id как шаблонные поля; исполнение — рантайм Airflow/БД."""

    template_fields = ("sql", "parameters")

    def __init__(
        self,
        *,
        sql: Any = None,
        conn_id: str | None = None,
        parameters: dict[str, Any] | None = None,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.parameters = parameters or {}
        self.database = database


class BranchSQLOperator(SQLExecuteQueryOperator):
    """Стаб ``BranchSQLOperator``: выбор веток разбирает движок прогона."""
