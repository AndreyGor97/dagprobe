"""Срез 27: 2.x-совместимость шима — реальные даги Airflow 2.7 (RealDag).

Импорт-пути 2.x (``airflow.operators.*``, ``airflow.decorators``,
``airflow.utils.*``, ``airflow.models.param``), ``airflow_clickhouse_plugin``,
TaskFlow-вызов внутри DAG (создаёт оператор, а не исполняет тело) и
``op >> task_fn(...)`` (XComArg справа от проводки) не должны падать.
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.analyze import analyze_file

DAG_27 = """\
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param, ParamsDict, DagParam
from airflow.operators.dummy import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

with DAG(
    dag_id="legacy_27",
    start_date=days_ago(2),
    schedule_interval="@daily",
    params={"conn": Param("clickhouse_default", type="string")},
) as dag:
    start = EmptyOperator(task_id="start")
    run = PythonOperator(task_id="run", python_callable=lambda: None)
    click = ClickHouseOperator(task_id="click", sql="SELECT 1")
    trigger = TriggerDagRunOperator(task_id="trigger", trigger_dag_id="other")

    @task()
    def fetch(**kwargs):
        hook = ClickHouseHook(clickhouse_conn_id=kwargs["params"]["conn"])
        return hook.execute("SELECT 1")

    fetched = fetch()
    start >> run >> fetched >> click >> trigger
    assert timezone.utcnow().tzinfo is not None
    assert isinstance({"p": ParamsDict()}, dict)
    assert DagParam(dag, "x").resolve() is None
"""


def test_legacy_27_dag_parses_without_errors(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "legacy_27.py").write_text(DAG_27, encoding="utf-8")

    result = analyze_file(dags / "legacy_27.py")

    assert result.dag_ids == ["legacy_27"], result.errors
    assert result.errors == []
