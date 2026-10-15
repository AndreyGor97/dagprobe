"""Shim ``...sensors.clickhouse_dbapi.ClickHouseSqlSensor``."""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class ClickHouseSqlSensor(BaseOperator):
    """Стаб сенсора: sql + poke-интервал хранятся; poke() — рантайм с ClickHouse."""

    template_fields = ("sql", "parameters", "database")

    def __init__(
        self,
        *,
        sql: Any = None,
        parameters: dict[str, Any] | None = None,
        database: str | None = None,
        clickhouse_conn_id: str = "clickhouse_default",
        poke_interval: float = 60.0,
        timeout: float = 7 * 24 * 60 * 60,
        mode: str = "poke",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.parameters = parameters or {}
        self.database = database
        self.clickhouse_conn_id = clickhouse_conn_id
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.mode = mode

    def poke(self, context: Any = None) -> bool:  # noqa: ARG002 — контекст не нужен
        """Как в плагине: непустой результат запроса = условие выполнено."""
        from dagprobe.shim.airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        return bool(
            ClickHouseHook(self.clickhouse_conn_id).get_records(self.sql, self.parameters or None)
        )
