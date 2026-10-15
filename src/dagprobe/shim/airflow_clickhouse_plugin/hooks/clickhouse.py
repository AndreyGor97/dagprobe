"""Shim ``airflow_clickhouse_plugin.hooks.clickhouse.ClickHouseHook`` — fixtures-хук (Срез 28).

Маршрутизация в Fixture Manager как у ``DbApiHook`` (Срез 4): Fake (``rows``) —
готовый ответ, SQL не исполняется; Emulated (``tables``) — настоящий SQL на
in-memory SQLite.
"""

from __future__ import annotations

from typing import Any

from dagprobe.fixtures import EmulatedConn


class ClickHouseHook:
    """Фикстурный хук: Fake отдаёт готовые rows, Emulated исполняет SQL на SQLite."""

    def __init__(self, clickhouse_conn_id: str = "clickhouse_default", **kwargs: Any) -> None:
        self.clickhouse_conn_id = clickhouse_conn_id
        for key, value in kwargs.items():
            setattr(self, key, value)

    def execute(self, sql: str, parameters: Any = None, **_: Any) -> Any:
        """Семантика clickhouse-driver ``Client.execute``: SELECT → список строк.

        Fake: готовые ``rows``; INSERT-пачка (parameters — список кортежей) —
        no-op (§8.1: Fake SQL не исполняет, ответа у INSERT нет).
        Emulated: исполняется по-настоящему; INSERT-пачка уходит в executemany —
        SQL плагина кончается на ``VALUES``, плейсхолдеры дописываем.
        """
        from dagprobe.shim import _intercept

        fixture = _intercept.fixtures().get(self.clickhouse_conn_id)
        batch = (
            isinstance(parameters, list)
            and bool(parameters)
            and isinstance(parameters[0], (tuple, list))
        )
        if not isinstance(fixture, EmulatedConn):
            return None if batch else list(fixture.rows)
        con = _intercept.emulated_connection(self.clickhouse_conn_id)
        if batch:
            rows = [tuple(r) for r in parameters]
            con.executemany(f"{sql} ({', '.join('?' * len(rows[0]))})", rows)
            con.commit()
            return None
        cur = con.execute(sql, parameters) if parameters else con.execute(sql)
        result = cur.fetchall() if cur.description is not None else []
        con.commit()
        return result

    def run(self, sql: str, parameters: Any = None) -> None:
        self.execute(sql, parameters)

    def get_records(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        """Строки-словари (как у ``DbApiHook.get_records``); сенсор проверяет непустоту."""
        from dagprobe.shim import _intercept

        fixture = _intercept.fixtures().get(self.clickhouse_conn_id)
        if not isinstance(fixture, EmulatedConn):
            return list(fixture.rows)
        con = _intercept.emulated_connection(self.clickhouse_conn_id)
        cur = con.execute(sql, parameters) if parameters else con.execute(sql)
        cols = [d[0] for d in cur.description or []]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def get_first_record(self, sql: str, parameters: Any = None) -> Any:
        records = self.get_records(sql, parameters)
        return records[0] if records else None

    def get_pandas_df(self, sql: str) -> Any:
        import pandas as pd

        return pd.DataFrame(self.get_records(sql))

    def get_conn(self) -> Any:
        from dagprobe.shim import _intercept

        return _intercept.emulated_connection(self.clickhouse_conn_id)
