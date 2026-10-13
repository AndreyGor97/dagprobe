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

EMPTY = LITERAL.replace("map_literal", "map_empty").replace("return [1, 2, 3]", "return []")

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


# --- expand_kwargs (Срез 14): каждый dict списка — kwargs одной раскрутки ---

XKW_LITERAL = """
from airflow.sdk import task, DAG

with DAG("xkw_literal", schedule=None, start_date=None, catchup=False) as dag:

    @task
    def add(x=0, y=0):
        return x + y

    @task
    def total(xs):
        return sum(xs)

    total(add.expand_kwargs([{"x": 1, "y": 10}, {"x": 2}, {"y": 5}]))
"""

XKW_UPSTREAM = XKW_LITERAL.replace("xkw_literal", "xkw_upstream").replace(
    '[{"x": 1, "y": 10}, {"x": 2}, {"y": 5}]',
    "make()",
)
XKW_UPSTREAM = XKW_UPSTREAM.replace(
    "    @task\n    def add",
    """    @task
    def make():
        return [{"x": 1}, {"x": 40}]

    @task
    def add""",
)

XKW_EMPTY = XKW_LITERAL.replace("xkw_literal", "xkw_empty").replace(
    '[{"x": 1, "y": 10}, {"x": 2}, {"y": 5}]',
    "[]",
)

XKW_BAD = XKW_LITERAL.replace("xkw_literal", "xkw_bad").replace(
    '[{"x": 1, "y": 10}, {"x": 2}, {"y": 5}]',
    '[{"x": 1}, "oops"]',
)


def test_expand_kwargs_literal(tmp_path: Path) -> None:
    dags = _write(tmp_path, XKW_LITERAL)
    run = run_dag(dags / "dag0.py", "xkw_literal")
    assert run.statuses == {"add": "ok", "total": "ok"}
    # 1+10, 2+0, 0+5 — недостающие kwargs берут дефолты, как в Airflow
    assert run.values["add"] == repr([11, 2, 5])
    assert run.values["total"] == repr(18)


def test_expand_kwargs_over_upstream_dicts(tmp_path: Path) -> None:
    dags = _write(tmp_path, XKW_UPSTREAM)
    run = run_dag(dags / "dag0.py", "xkw_upstream")
    # upstream вернул список словарей — рёбра зависимостей по XComArg
    assert run.statuses == {"make": "ok", "add": "ok", "total": "ok"}
    assert run.values["add"] == repr([1, 40])


def test_expand_kwargs_empty_skips(tmp_path: Path) -> None:
    dags = _write(tmp_path, XKW_EMPTY)
    run = run_dag(dags / "dag0.py", "xkw_empty")
    assert run.statuses == {"add": "skipped", "total": "skipped"}


def test_expand_kwargs_non_dict_is_loud(tmp_path: Path) -> None:
    dags = _write(tmp_path, XKW_BAD)
    run = run_dag(dags / "dag0.py", "xkw_bad")
    # как в Airflow: невалидный литерал — ошибка парсинга DAG, не рантайма
    assert run.statuses == {}
    assert "list of dicts" in run.failures["<setup>"]


# --- классические операторы: PythonOperator.partial(...).expand(...)/expand_kwargs(...) ---

CLASSIC = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, DAG

with DAG("classic_expand", schedule=None, start_date=None, catchup=False) as dag:

    @task
    def make():
        return [1, 2, 3]

    def double(x):
        return x * 2

    def add(x, base=0):
        return x + base

    def pw(x, y):
        return x ** y

    @task
    def total(xs):
        return sum(xs)

    total(PythonOperator.partial(task_id="double", python_callable=double).expand(x=make()))
"""

_LINE = 'total(PythonOperator.partial(task_id="double", python_callable=double).expand(x=make()))'

CLASSIC_STATIC = CLASSIC.replace("classic_expand", "classic_static").replace(
    _LINE,
    "add_ = PythonOperator.partial(\n"
    '        task_id="add", python_callable=add, op_kwargs={"base": 100}\n'
    "    ).expand(x=[1, 2])",
)

CLASSIC_XKW = CLASSIC.replace("classic_expand", "classic_xkw").replace(
    _LINE,
    'pw_ = PythonOperator.partial(task_id="pw", python_callable=pw).expand_kwargs(\n'
    '        [{"x": 2, "y": 3}, {"x": 5, "y": 2}]\n'
    "    )",
)

# каноническая форма Airflow для классических операторов: expand(op_kwargs=[{...}])
CLASSIC_OPKW = CLASSIC.replace("classic_expand", "classic_opkw").replace(
    _LINE,
    'add_ = PythonOperator.partial(task_id="add", python_callable=add).expand(\n'
    '        op_kwargs=[{"x": 1, "base": 10}, {"x": 2}]\n'
    "    )",
)


def test_classic_partial_expand_over_upstream(tmp_path: Path) -> None:
    dags = _write(tmp_path, CLASSIC)
    run = run_dag(dags / "dag0.py", "classic_expand")
    assert run.statuses == {"make": "ok", "double": "ok", "total": "ok"}
    assert run.values["double"] == repr([2, 4, 6])
    assert run.values["total"] == repr(12)


def test_classic_partial_static_op_kwargs(tmp_path: Path) -> None:
    dags = _write(tmp_path, CLASSIC_STATIC)
    run = run_dag(dags / "dag0.py", "classic_static")
    assert run.statuses["add"] == "ok"
    assert run.values["add"] == repr([101, 102])  # static base=100 из partial


def test_classic_partial_expand_kwargs(tmp_path: Path) -> None:
    dags = _write(tmp_path, CLASSIC_XKW)
    run = run_dag(dags / "dag0.py", "classic_xkw")
    assert run.statuses["pw"] == "ok"
    assert run.values["pw"] == repr([8, 25])


def test_classic_expand_op_kwargs_form(tmp_path: Path) -> None:
    dags = _write(tmp_path, CLASSIC_OPKW)
    run = run_dag(dags / "dag0.py", "classic_opkw")
    # expand(op_kwargs=[{...}]): dict элемента перекрывает static, недостающее — дефолт
    assert run.statuses["add"] == "ok"
    assert run.values["add"] == repr([11, 2])
