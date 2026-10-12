"""Срез 3 (§6.5 Task Test, §6.4 Jinja Previewer, §8.1 Fake-фикстуры).

DAG — временный файл: extract (PythonOperator + DbApiHook conn_id) →
transform (@task, XComArg) → load (@task, assert). Прогон идёт в subprocess,
как в бою; проверяем реальные статусы, значения XCom и громкий отказ без фикстуры.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from dagprobe.analyze import analyze_file
from dagprobe.fixtures import FixtureNotFound, Fixtures
from dagprobe.task_test import run_dag

DAG_SOURCE = """
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import DbApiHook

with DAG("etl", schedule=None, start_date=None, catchup=False) as dag:
    def extract(**_):
        return DbApiHook(conn_id="dwh").get_records("SELECT amount FROM t")

    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    @task
    def transform(rows):
        return [r["amount"] * 2 for r in rows]

    @task
    def load(doubled):
        assert doubled == [20, 40], doubled
        return "loaded"

    load(transform(extract_task.output))
"""

TEMPLATED_DAG = """
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG("tpl", schedule=None, start_date=None, catchup=False) as dag:
    BashOperator(task_id="echo", bash_command="echo {{ ds }} + {{ macros.ds_add(ds, 7) }}")
"""

FIXTURES_YAML = """
conns:
  dwh:
    conn_type: postgres
    rows:
      - {amount: 10}
      - {amount: 20}
"""


@pytest.fixture()
def dag_file(tmp_path: Path) -> Path:
    f = tmp_path / "etl.py"
    f.write_text(textwrap.dedent(DAG_SOURCE), encoding="utf-8")
    return f


@pytest.fixture()
def fixtures_file(tmp_path: Path) -> Path:
    f = tmp_path / "fixtures.yaml"
    f.write_text(FIXTURES_YAML, encoding="utf-8")
    return f


def test_task_test_runs_chain_with_hook_fixture(dag_file: Path, fixtures_file: Path) -> None:
    """§6.5: PythonOperator + @task-цепочка, XCom по рёбрам, фикстура conn_id."""
    result = run_dag(dag_file, "etl", fixtures_path=fixtures_file)
    assert result.failures == {}
    assert result.statuses == {"extract": "ok", "transform": "ok", "load": "ok"}
    assert result.values["extract"] == repr([{"amount": 10}, {"amount": 20}])
    assert result.values["transform"] == "[20, 40]"
    assert result.values["load"] == "'loaded'"


def test_task_test_subset_only_runs_selected(dag_file: Path, fixtures_file: Path) -> None:
    result = run_dag(dag_file, "etl", task_ids=["extract"], fixtures_path=fixtures_file)
    assert result.statuses == {"extract": "ok"}


def test_task_test_missing_fixture_fails_loudly(dag_file: Path) -> None:
    """§8.6: без фикстуры conn_id — громкий отказ, а не молчаливый поход в реальную систему."""
    result = run_dag(dag_file, "etl")
    assert result.statuses["extract"] == "failed"
    assert "FixtureNotFound" in result.failures["extract"]


def test_task_test_reports_task_failure(tmp_path: Path) -> None:
    """Падение в теле задачи → статус failed + traceback, не падение прогона."""
    bad = tmp_path / "bad.py"
    bad.write_text(
        textwrap.dedent(
            """
            from airflow.sdk import DAG
            from airflow.providers.standard.operators.python import PythonOperator

            with DAG("b", schedule=None, start_date=None, catchup=False) as dag:
                def boom():
                    raise ValueError("намеренный сбой")

                PythonOperator(task_id="boom", python_callable=boom)
            """
        ),
        encoding="utf-8",
    )
    result = run_dag(bad, "b")
    assert result.statuses["boom"] == "failed"
    assert "намеренный сбой" in result.failures["boom"]


def test_jinja_preview_renders_template_fields(tmp_path: Path) -> None:
    """§6.4: render=True даёт before/after шаблонов с фиксированной logical_date."""
    f = tmp_path / "tpl.py"
    f.write_text(textwrap.dedent(TEMPLATED_DAG), encoding="utf-8")
    result = analyze_file(f, render=True)
    rendered = result.dags[0].rendered["echo"]["bash_command"]
    assert "{{ ds }}" in rendered["before"]
    assert "2026-09-29" in rendered["after"]
    assert "2026-10-06" in rendered["after"]  # macros.ds_add(ds, 7)


def test_fixtures_load_roundtrip(fixtures_file: Path) -> None:
    fx = Fixtures.load(fixtures_file)
    assert fx.get("dwh").rows == [{"amount": 10}, {"amount": 20}]
    with pytest.raises(FixtureNotFound):
        fx.get("unknown")


def test_legacy_root_import_and_dirty_stdout(tmp_path: Path) -> None:
    """Регресс BACKLOG: ``from airflow import DAG`` (валидный код 2→3) + print() в DAG.

    Грязный stdout при импорте не должен ломать JSON-обмен с child'ом.
    """
    f = tmp_path / "legacy.py"
    f.write_text(
        textwrap.dedent(
            """
            import sys
            print("шум в stdout при импорте")
            sys.stdout.write("без перевода строки")
            from airflow import DAG
            from airflow.providers.standard.operators.bash import BashOperator

            with DAG("legacy", schedule=None, start_date=None, catchup=False) as dag:
                BashOperator(task_id="t", bash_command="echo hi")
            """
        ),
        encoding="utf-8",
    )
    result = analyze_file(f)
    assert result.errors == []
    assert result.dag_ids == ["legacy"]
