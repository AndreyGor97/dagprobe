"""Срез 29: шим-дыры, найденные на RealDag2 — @dag/task_group в decorators,
sensors (base + external_task), operators.empty, Dataset/Variable в корне,
модели (DAG/Param) и utils (task_group/trigger_rule/state/weekday).

Acceptance: мини-даг со всеми новыми импортами парсится без ошибок; легаси
(элементы `contrib`, удалённые в Airflow 3) осознанно НЕ шимится — примечание:
решение Среза 29 пересмотрено в Срезе 30 для 2.x-реэкспортов операторов
(``dummy_operator`` и пр.) — корпус RealDag2 просит их десятками раз.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dagprobe.analyze import analyze_file
from dagprobe.shim.airflow.utils.state import DagRunState
from dagprobe.shim.airflow.utils.trigger_rule import TriggerRule
from dagprobe.shim.airflow.utils.weekday import WeekDay

MINI = """
from datetime import datetime

from airflow import DAG, Dataset, Variable
from airflow.decorators import dag, task, task_group
from airflow.models import DAG as ModelsDAG, Param
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weekday import WeekDay

start = datetime(2026, 10, 1)

with DAG(
    dag_id="s29_classic",
    start_date=start,
    schedule=None,
    params={"answer": Param(42)},
    default_args={"trigger_rule": TriggerRule.ALL_SUCCESS},
) as d:
    wait = ExternalTaskSensor(
        task_id="wait", external_dag_id="other", external_task_id="leaf", mode="reschedule"
    )
    marker = ExternalTaskMarker(task_id="marker", external_task_id="leaf")
    with TaskGroup("group1"):
        EmptyOperator(task_id="do")
    wait >> marker
    ds = Dataset("s3://bucket/key")


@dag(start_date=start, schedule=None, tags=["s29"])
def s29_taskflow():

    @task
    def read_var():
        return Variable.get("missing", default_var="fallback")

    @task_group
    def grp():
        return read_var()

    grp()


s29_taskflow()
"""


def _write(tmp_path: Path, name: str, text: str) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    file = dags / f"{name}.py"
    file.write_text(text, encoding="utf-8")
    return file


def test_all_new_imports_parse_clean(tmp_path: Path) -> None:
    result = analyze_file(_write(tmp_path, "mini29", MINI))
    assert result.errors == [], [e.message for e in result.errors]
    assert set(result.dag_ids) == {"s29_classic", "s29_taskflow"}
    classic = next(d for d in result.dags if d.dag_id == "s29_classic")
    assert set(classic.tasks) == {"wait", "marker", "group1.do"}


def test_enum_semantics() -> None:
    # str-Enum: сравнение со строками движка работает
    assert TriggerRule.ALL_SUCCESS == "all_success"
    assert DagRunState.is_completed(DagRunState.SUCCESS)
    assert not DagRunState.is_completed(DagRunState.RUNNING)
    assert WeekDay.FRIDAY.full_name == "Friday"
    assert WeekDay.from_string("monday") is WeekDay.MONDAY


def test_variable_get_default_vs_loud() -> None:
    from dagprobe.shim._core import Variable

    assert Variable.get("missing", default_var="fallback") == "fallback"
    with pytest.raises(RuntimeError, match="missing"):
        Variable.get("missing")
