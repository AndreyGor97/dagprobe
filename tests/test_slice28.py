"""Срез 28: fixtures-хук ClickHouse — ClickHouseOperator/ClickHouseSqlSensor в прогоне.

ClickHouseHook маршрутизируется в Fixture Manager (Fake — готовые rows,
Emulated — SQL на SQLite, как DbApiHook в Срезе 4); движок прогона исполняет
ClickHouseOperator и один poke ClickHouseSqlSensor. Acceptance — боевой даг
RealDag ``clhs_inserts_count`` целиком на Fake-фикстурах.
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.task_test import run_dag

MINI = """\
from datetime import datetime

from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.sensors.clickhouse_dbapi import ClickHouseSqlSensor

with DAG(dag_id="clhs_mini", start_date=datetime(2026, 9, 29), schedule=None) as dag:
    put = ClickHouseOperator(
        task_id="put",
        sql="INSERT INTO events (id, name) VALUES (1, 'a')",
        clickhouse_conn_id="ch",
    )
    sense_true = ClickHouseSqlSensor(
        task_id="sense_true",
        sql="SELECT id FROM events WHERE name = 'a'",
        clickhouse_conn_id="ch",
    )
    sense_false = ClickHouseSqlSensor(
        task_id="sense_false",
        sql="SELECT id FROM events WHERE name = 'nope'",
        clickhouse_conn_id="ch",
    )
    put >> sense_true
"""

TASKFLOW = """\
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

with DAG(dag_id="ctx_mini", start_date=datetime(2026, 9, 29), schedule=None) as dag:
    @task()
    def fetch(params=None, **kwargs):
        hook = ClickHouseHook(clickhouse_conn_id="fake_conn")
        row = hook.execute("select n from t")[0]
        kwargs["ti"].xcom_push(key="n", value=row["n"])
        return params["answer"]

    @task()
    def show(v, **context):
        return (
            context["data_interval_end"].to_iso8601_string(),
            context["ti"].xcom_pull(task_ids="fetch", key="n"),
            v,
        )

    show(fetch(params={"answer": 42}))
"""

FAKE_FIXTURES = """\
conns:
  fake_conn:
    conn_type: clickhouse
    rows:
      - n: 5
"""

EMULATED_FIXTURES = """\
conns:
  ch:
    conn_type: clickhouse
    tables:
      events:
        - id: 0
          name: seed
"""

REALDAG_FILE = (
    Path(__file__).resolve().parents[1] / "RealDag" / "clhs_inserts_count" / "clhs_inserts_count.py"
)

# один rows-набор обслуживает оба запроса дага (Fake не смотрит в SQL):
# крайний timestamp_hour для clhs_get_last_ts и counts-строки для merge
REALDAG_FIXTURES = """\
conns:
  clhs_lk5:
    conn_type: clickhouse
    rows:
      - timestamp_hour: 2026-09-29 10:00:00+03:00
        ts: '2026-09-29 09:00:00'
        table_name: 'base.client'
        row_count_dc2: 10
      - timestamp_hour: 2026-09-29 10:00:00+03:00
        ts: '2026-09-29 09:00:00'
        table_name: 'base.application'
        row_count_dc2: 20
  clhs_lk2:
    conn_type: clickhouse
    rows:
      - ts: '2026-09-29 09:00:00'
        table_name: 'base.client'
        row_count_dc2: 11
      - ts: '2026-09-29 09:00:00'
        table_name: 'base.other_table'
        row_count_dc2: 5
"""


def _write(tmp_path: Path, name: str, text: str) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    file = dags / f"{name}.py"
    file.write_text(text, encoding="utf-8")
    return file


def test_clickhouse_operator_and_sensor_on_emulated(tmp_path: Path) -> None:
    fixtures = tmp_path / "fx.yaml"
    fixtures.write_text(EMULATED_FIXTURES, encoding="utf-8")

    run = run_dag(_write(tmp_path, "clhs_mini", MINI), "clhs_mini", fixtures_path=fixtures)

    assert run.statuses == {"put": "ok", "sense_true": "ok", "sense_false": "failed"}
    assert "poke returned False" in run.failures["sense_false"]


def test_taskflow_context_params_and_hook_on_fake(tmp_path: Path) -> None:
    fixtures = tmp_path / "fx.yaml"
    fixtures.write_text(FAKE_FIXTURES, encoding="utf-8")

    run = run_dag(_write(tmp_path, "ctx_mini", TASKFLOW), "ctx_mini", fixtures_path=fixtures)

    assert run.statuses == {"fetch": "ok", "show": "ok"}, run.failures
    # data_interval_end — pendulum (to_iso8601_string), xcom_push по ключу, params=
    assert "2026-09-29T00:00:00" in run.values["show"]


def test_realdag_clhs_inserts_count_runs_end_to_end(tmp_path: Path) -> None:
    fixtures = tmp_path / "fx.yaml"
    fixtures.write_text(REALDAG_FIXTURES, encoding="utf-8")

    run = run_dag(REALDAG_FILE, "CLHS_inserts_count", fixtures_path=fixtures)

    assert run.statuses == {
        "start_task": "ok",
        "clhs_get_last_ts": "ok",
        "clhs_get_merge_insert_counts_by_ts": "ok",
        "end_task": "ok",
    }, run.failures
