"""Срез 4: DB-стаб на Emulated Engine (§8.2) — настоящий SQL без внешней СУБД.

Проверяем ценностное предложение среза: JOIN/GROUP BY/HAVING исполняются
по-настоящему на in-memory SQLite, а не подменяются заранее заданным ответом.
"""

from __future__ import annotations

import textwrap
from datetime import date
from pathlib import Path

import pytest

from dagprobe.fixtures import EmulatedConn, Fixtures
from dagprobe.shim import _intercept
from dagprobe.shim.airflow.sdk.bases.hook import DbApiHook
from dagprobe.task_test import run_dag

DAG_SOURCE = """
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import DbApiHook

with DAG("join_etl", schedule=None, start_date=None, catchup=False) as dag:
    def top_customers(**_):
        return DbApiHook(conn_id="dwh").get_records(
            "SELECT c.name, SUM(o.amount) AS total"
            " FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
            " GROUP BY c.name HAVING SUM(o.amount) > 10 ORDER BY total DESC"
        )

    PythonOperator(task_id="top", python_callable=top_customers)
"""

BAD_SQL_DAG = """
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import DbApiHook

with DAG("bad_sql", schedule=None, start_date=None, catchup=False) as dag:
    def drift(**_):
        return DbApiHook(conn_id="dwh").get_records("SELECT missing_col FROM orders")

    PythonOperator(task_id="drift", python_callable=drift)
"""

FIXTURES_YAML = """
conns:
  dwh:
    conn_type: postgres
    tables:
      customers:
        - {customer_id: 10, name: Alice}
        - {customer_id: 20, name: Bob}
      orders:
        - {order_id: 1, customer_id: 10, amount: 100}
        - {order_id: 2, customer_id: 10, amount: 5}
        - {order_id: 3, customer_id: 20, amount: 7}
"""


@pytest.fixture()
def fixtures_file(tmp_path: Path) -> Path:
    f = tmp_path / "fixtures.yaml"
    f.write_text(textwrap.dedent(FIXTURES_YAML), encoding="utf-8")
    return f


def _dag(tmp_path: Path, name: str, source: str) -> Path:
    f = tmp_path / f"{name}.py"
    f.write_text(textwrap.dedent(source), encoding="utf-8")
    return f


def test_emulated_engine_joins_for_real(tmp_path: Path, fixtures_file: Path) -> None:
    """§8.2: JOIN + GROUP BY + HAVING считаются движком, а не берутся из фикстуры."""
    result = run_dag(
        _dag(tmp_path, "join_etl", DAG_SOURCE), "join_etl", fixtures_path=fixtures_file
    )
    assert result.failures == {}
    assert result.statuses == {"top": "ok"}
    assert result.values["top"] == repr([{"name": "Alice", "total": 105}])


def test_emulated_engine_catches_schema_drift(tmp_path: Path, fixtures_file: Path) -> None:
    """§8.2 Schema Drift: несуществующая колонка — ошибка движка, а не молчаливый Fake."""
    result = run_dag(_dag(tmp_path, "bad_sql", BAD_SQL_DAG), "bad_sql", fixtures_path=fixtures_file)
    assert result.statuses["drift"] == "failed"
    assert "missing_col" in result.failures["drift"]


def test_emulated_connection_is_shared_within_run(fixtures_file: Path) -> None:
    """Одно соединение на прогон: DDL/DML одной задачи видна следующей (§8.2 run())."""
    _intercept.set_fixtures(Fixtures.load(fixtures_file))
    try:
        hook = DbApiHook(conn_id="dwh")
        hook.run("INSERT INTO orders VALUES (4, 20, 100)")
        rows = hook.get_records(
            "SELECT c.name, SUM(o.amount) AS total FROM orders o"
            " JOIN customers c ON o.customer_id = c.customer_id"
            " GROUP BY c.name ORDER BY c.name"
        )
        assert rows == [{"name": "Alice", "total": 105}, {"name": "Bob", "total": 107}]
    finally:
        _intercept.clear_fixtures()


def test_fixtures_reject_rows_and_tables_together(tmp_path: Path) -> None:
    """Громкая ошибка загрузки вместо молчаливого выбора одного из двух уровней."""
    f = tmp_path / "bad.yaml"
    f.write_text(
        "conns:\n  dwh:\n    rows: [{a: 1}]\n    tables:\n      t: [{a: 1}]\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="не оба сразу"):
        Fixtures.load(f)


def test_emulated_engine_binds_yaml_dates_and_nulls() -> None:
    """Значения из YAML (даты, None, bool) доезжают до SQLite и читаются обратно."""
    conn = EmulatedConn(
        conn_id="dwh",
        conn_type="postgres",
        tables={
            "events": [
                {"day": date(2026, 1, 2), "n": None, "ok": True},
                {"day": date(2026, 1, 3), "n": 5, "ok": False},
            ]
        },
    )
    con = conn.connect()
    rows = con.execute("SELECT day, n, ok FROM events WHERE n IS NULL").fetchall()
    con.close()
    assert rows == [("2026-01-02", None, 1)]
