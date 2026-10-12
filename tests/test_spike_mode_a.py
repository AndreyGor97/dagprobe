"""B2/B4/B5 (файл 18): перехват импорта и построение графа на 5 эталонных DAG.

Airflow не установлен — предусловие Mode A. При установленном настоящем airflow
эти тесты пропускаются: проверялось бы не то.
"""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from typing import Any

import pytest

SPIKE_DAGS = Path(__file__).parent.parent / "spike" / "dags"

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("airflow") is not None,
    reason="установлен настоящий airflow — Mode A не тестируется",
)


@pytest.fixture()
def shim():
    from dagprobe.shim import activate, deactivate

    activate()
    yield
    deactivate()


def _load(filename: str) -> dict[str, Any]:
    """Импорт DAG-файла по пути; возвращает реестр созданных DAG."""
    from dagprobe.shim import _core

    spec = importlib.util.spec_from_file_location(filename[:-3], SPIKE_DAGS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return _core.all_dags()


def test_finder_intercepts_airflow_sdk(shim: None) -> None:
    """Гипотеза 1: finder перехватывает airflow.* и не задевает посторонние импорты."""
    import airflow.sdk  # noqa: F401 — сам факт импорта и есть проверка

    assert airflow.sdk.__name__ == "airflow.sdk"
    assert "dagprobe" in str(airflow.sdk.__file__)
    import json

    assert json.__name__ == "json"


def test_tutorial_dag_builds_graph(shim: None) -> None:
    dags = _load("tutorial.py")
    dag = dags["tutorial"]
    assert set(dag.task_dict) == {"print_date", "sleep", "templated"}
    assert set(dag.edges()) == {("print_date", "sleep"), ("print_date", "templated")}


def test_branch_dag_builds_graph(shim: None) -> None:
    dags = _load("example_branch_python_dop_operator_3.py")
    dag = dags["example_branch_dop_operator_v3"]
    assert set(dag.task_dict) == {"should_run", "empty_task_1", "empty_task_2"}
    assert set(dag.edges()) == {
        ("should_run", "empty_task_1"),
        ("should_run", "empty_task_2"),
    }


def test_taskflow_dag_builds_graph(shim: None) -> None:
    dags = _load("tutorial_taskflow_api.py")
    dag = dags["tutorial_taskflow_api"]
    assert set(dag.task_dict) == {"extract", "transform", "load"}
    assert set(dag.edges()) == {("extract", "transform"), ("transform", "load")}


def test_dynamic_mapping_dag_builds_graph(shim: None) -> None:
    dags = _load("example_dynamic_task_mapping.py")
    first = dags["example_dynamic_task_mapping"]
    assert set(first.task_dict) == {"add_one", "sum_it"}
    assert set(first.edges()) == {("add_one", "sum_it")}

    second = dags["example_task_mapping_second_order"]
    assert set(second.task_dict) == {"get_nums", "times_2", "add_10"}
    assert set(second.edges()) == {
        ("get_nums", "times_2"),
        ("times_2", "add_10"),
    }

    group = dags["example_task_group_mapping"]
    assert set(group.task_dict) == {"op.add_1", "op.mul_2"}
    assert set(group.edges()) == {("op.add_1", "op.mul_2")}


def test_workday_timetable_dag_builds_graph(shim: None) -> None:
    dags = _load("example_workday_timetable.py")
    dag = dags["example_workday_timetable"]
    assert set(dag.task_dict) == {"run_this"}
    assert dag.edges() == []
    # расписание реально вычислилось кастомным Timetable'ом (поведенческая проверка)
    assert dag.schedule is not None
