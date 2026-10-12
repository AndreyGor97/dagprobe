"""Shim: ``airflow.sdk.bases.hook`` — точка перехвата фикстур (§8.1, Срезы 3–4).

``get_connection`` отдаёт Connection, собранный из Fake-фикстуры; реального
секретодоступа в шиме нет и не должно быть (§8.6 safe-by-default).

``DbApiHook`` работает на двух уровнях §8.1: ``rows`` — Fake (SQL не исполняется),
``tables`` — Emulated Engine (§8.2, настоящий SQL на in-memory SQLite).
"""

from __future__ import annotations

from typing import Any

from dagprobe.fixtures import EmulatedConn


class Connection:  # noqa: D101 — минимальный shim airflow Connection
    def __init__(self, conn_id: str, conn_type: str, **kw: Any) -> None:
        self.conn_id = conn_id
        self.conn_type = conn_type
        for k, v in kw.items():
            setattr(self, k, v)

    def get_uri(self) -> str:
        return f"{self.conn_type}://fake"


class BaseHook:
    """Shim ``BaseHook``: ``get_connection`` идёт в Fixture Registry, не в секреты."""

    @staticmethod
    def get_connection(conn_id: str) -> Connection:
        from dagprobe.shim import _intercept

        fake = _intercept.fixtures().get(conn_id)
        return Connection(conn_id=fake.conn_id, conn_type=fake.conn_type)


class DbApiHook(BaseHook):
    """Shim ``DbApiHook``: Fake отдаёт готовые rows, Emulated исполняет SQL (§8.2)."""

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id

    def get_records(self, sql: str) -> list[dict[str, Any]]:
        from dagprobe.shim import _intercept

        conn = _intercept.fixtures().get(self.conn_id)
        if not isinstance(conn, EmulatedConn):
            return list(conn.rows)  # Fake: заранее заданный ответ, sql не исполняется
        cur = _intercept.emulated_connection(self.conn_id).execute(sql)
        cols = [d[0] for d in cur.description or []]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def get_pandas_df(self, sql: str) -> Any:
        """То же, что ``get_records``, но DataFrame (pandas пришёл зависимостью в Срезе 5)."""
        import pandas as pd

        return pd.DataFrame(self.get_records(sql))

    def run(self, sql: str) -> None:
        """Emulated: DDL/DML на том же соединении прогона (§8.2); с Fake-фикстурой — ошибка."""
        from dagprobe.shim import _intercept

        con = _intercept.emulated_connection(self.conn_id)
        con.execute(sql)
        con.commit()

    def insert_rows(  # noqa: PLR0913 — сигнатура Airflow
        self,
        table: str,
        rows: Any,
        target_fields: list[str] | None = None,
        commit_every: int = 1000,  # noqa: ARG002 — неуместно на in-memory SQLite
        replace: bool = False,  # noqa: ARG002 — INSERT OR REPLACE: когда понадобится тесту
        executemany: bool = False,  # noqa: ARG002 — всегда executemany, это наш движок
    ) -> None:
        """Пакетный INSERT (как в Airflow) через Emulated Engine; Fake — громкая ошибка.

        Строки — последовательности значений (столбцы = ``target_fields``) либо
        словари (столбцы из ключей). Нет фикстуры → FixtureNotFound, Fake
        (``rows``) → ValueError — молча «вставлять» некуда (§8.6).
        """
        from dagprobe.shim import _intercept

        con = _intercept.emulated_connection(self.conn_id)
        if target_fields and rows and isinstance(rows[0], dict):
            data = [tuple(r[f] for f in target_fields) for r in rows]
        else:
            data = [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in rows]
        if not data:
            return
        cols = f" ({', '.join(target_fields)})" if target_fields else ""
        placeholders = ", ".join("?" * len(data[0]))
        # ponytail: одна пачка одним executemany; commit_every/replace — воздушные,
        # на in-memory SQLite прогона смысл появятся только с чанкованной записью
        con.executemany(f'INSERT INTO "{table}"{cols} VALUES ({placeholders})', data)
        con.commit()
