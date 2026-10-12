"""Срез 11: dynamic task mapping (``.expand()``) в прогоне.

Граф-сторона (шим, пометка ``mapped``, рёбра) — со спайка; здесь — рантайм:
callable исполняется раз на элемент, результат задачи — список, downstream
видит список. Прогоны — subprocess, как в Срезах 7/10.
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.task_test import run_dag

LITERAL = """
from airflow.sdk import task, DAG

with DAG("map_literal", schedule=None, start_date=None, catchup=False) as dag:

    @task
    def make():
        return [1, 2, 3]

    @task
    def add_one(x):
        return x + 1

    @task
    def total(xs):
        return sum(xs)

    total(add_one.expand(x=make()))
"""

UPSTREAM = LITERAL.replace("map_literal", "map_upstream").replace(
    "add_one.expand(x=make())", "add_one.expand(x=[10, 20])"
)

EMPTY = LITERAL.replace("map_literal", "map_empty").replace(
    "return [1, 2, 3]", "return []"
)

FAILING = """
from airflow.sdk import task, DAG

with DAG("map_failing", schedule=None, start_date=None, catchup=False) as dag:

    @task
    def make():
        return [1, 2, 3]

    @task
    def boom(x):
        if x == 2:
            raise ValueError("нарочно на 2")
        return x

    boom.expand(x=make())
"""


def _write(tmp_path: Path, *sources: str) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    for i, src in enumerate(sources):
        (dags / f"dag{i}.py").write_text(src, encoding="utf-8")
    return dags


def test_expand_literal_list(tmp_path: Path) -> None:
    dags = _write(tmp_path, LITERAL)
    run = run_dag(dags / "dag0.py", "map_literal")
    assert run.statuses == {"make": "ok", "add_one": "ok", "total": "ok"}
    # каждая раскрутка со своим элементом; downstream видит список результатов
    assert run.values["add_one"] == repr([2, 3, 4])
    assert run.values["total"] == repr(9)


def test_expand_over_upstream_list(tmp_path: Path) -> None:
    dags = _write(tmp_path, UPSTREAM)
    run = run_dag(dags / "dag0.py", "map_upstream")
    # make() в этой DAG не вызывается — expand идёт по литералу [10, 20]
    assert run.statuses == {"add_one": "ok", "total": "ok"}
    assert run.values["add_one"] == repr([11, 21])


def test_expand_empty_list_skips(tmp_path: Path) -> None:
    dags = _write(tmp_path, EMPTY)
    run = run_dag(dags / "dag0.py", "map_empty")
    # 0 раскруток: mapped-задача skipped (как в Airflow), downstream тоже
    assert run.statuses == {"make": "ok", "add_one": "skipped", "total": "skipped"}


def test_expand_instance_failure_is_loud(tmp_path: Path) -> None:
    dags = _write(tmp_path, FAILING)
    run = run_dag(dags / "dag0.py", "map_failing")
    assert run.statuses["boom"] == "failed"
    assert "map_index=1" in run.failures["boom"]  # нарочно на элементе 2 (индекс 1)
    assert "нарочно" in run.failures["boom"]
