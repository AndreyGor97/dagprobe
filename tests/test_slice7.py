"""Срез 7: ветвление (BranchPythonOperator) и trigger_rule в прогоне + insert_rows.

Таблица trigger_rule проверяется в процессе (без subprocess — быстро); прогоны
с subprocess — по одному сценарию на класс поведения, как в Срезах 3–5.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dagprobe.cli import main
from dagprobe.task_test import branch_targets, run_dag, trigger_rule_decide

FIXTURES = """
conns:
  dwh:
    conn_type: postgres
    tables:
      events:
        - {event: signup, n: 3}
  fake_conn:
    conn_type: postgres
    rows:
      - {x: 1}
"""

BRANCH_DAG = """
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from airflow.sdk.bases.hook import DbApiHook

with DAG("branch_etl", schedule=None, start_date=None, catchup=False) as dag:

    def extract(**_):
        return DbApiHook(conn_id="dwh").get_records("SELECT * FROM events")

    def route(rows, **_):
        return "load_events" if rows else "load_empty"

    def load_events(rows, **_):
        # таблицу создаём сами: фикстура даёт только events, INSERT в несуществующую — ошибка
        DbApiHook(conn_id="dwh").run("CREATE TABLE IF NOT EXISTS events_loaded (event, n)")
        DbApiHook(conn_id="dwh").insert_rows("events_loaded", rows, target_fields=["event", "n"])
        return f"loaded {len(rows)}"

    def report(msg, **_):
        c = DbApiHook(conn_id="dwh").get_records("SELECT COUNT(*) AS c FROM events_loaded")
        return f"{msg}; таблица содержит {c[0]['c']}"

    extract_t = PythonOperator(task_id="extract", python_callable=extract)
    route_t = BranchPythonOperator(
        task_id="route", python_callable=route, op_kwargs={"rows": extract_t.output}
    )
    load_empty = PythonOperator(
        task_id="load_empty", python_callable=lambda **_: "пусто", trigger_rule="all_failed"
    )
    load_events_t = PythonOperator(
        task_id="load_events", python_callable=load_events, op_kwargs={"rows": extract_t.output}
    )
    report_t = PythonOperator(
        task_id="report", python_callable=report, op_kwargs={"msg": load_events_t.output}
    )

    extract_t >> route_t >> [load_events_t, load_empty]
    load_events_t >> report_t
"""

CASCADE_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("cascade", schedule=None, start_date=None, catchup=False) as dag:

    def boom(**_):
        raise ValueError("нарочно")

    def ok(**_):
        return "ok"

    t1 = PythonOperator(task_id="t1", python_callable=boom)
    t2 = PythonOperator(task_id="t2", python_callable=ok)  # all_success за упавшим
    t3 = PythonOperator(task_id="t3", python_callable=ok, trigger_rule="none_failed")  # за t2
    t4 = PythonOperator(task_id="t4", python_callable=ok, trigger_rule="always")  # всегда ок

    t1 >> t2 >> t3
    t1 >> t4
"""

BRANCH_NONE_DAG = """
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG

with DAG("branch_none", schedule=None, start_date=None, catchup=False) as dag:

    def route(**_):
        return None  # по Airflow: ни одна ветка не выбрана

    route_t = BranchPythonOperator(task_id="route", python_callable=route)
    a = PythonOperator(task_id="a", python_callable=lambda **_: 1)
    b = PythonOperator(task_id="b", python_callable=lambda **_: 1)
    route_t >> [a, b]
"""

BRANCH_WRONG_DAG = """
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG

with DAG("branch_wrong", schedule=None, start_date=None, catchup=False) as dag:

    def route(**_):
        return "не_задача"

    route_t = BranchPythonOperator(task_id="route", python_callable=route)
    a = PythonOperator(task_id="a", python_callable=lambda **_: 1)
    route_t >> a
"""

INSERT_FAKE_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.bases.hook import DbApiHook

with DAG("insert_fake", schedule=None, start_date=None, catchup=False) as dag:

    def ins(**_):
        DbApiHook(conn_id="fake_conn").insert_rows("t", [(1,)])

    PythonOperator(task_id="ins", python_callable=ins)
"""


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "dags.py").write_text(
        BRANCH_DAG + CASCADE_DAG + BRANCH_NONE_DAG + BRANCH_WRONG_DAG + INSERT_FAKE_DAG,
        encoding="utf-8",
    )
    fx = tmp_path / "fixtures.yaml"
    fx.write_text(FIXTURES, encoding="utf-8")
    return dags, fx


# --- таблица trigger_rule: в процессе, без subprocess --------------------------

CASES = [
    # rule, статусы upstream → вердикт
    ("all_success", ["ok", "ok"], "run"),
    ("all_success", ["ok", "failed"], "upstream_failed"),
    ("all_success", ["ok", "skipped"], "skipped"),  # как Airflow: скип без фейлов → SKIPPED
    ("all_failed", ["failed", "failed"], "run"),
    ("all_failed", ["ok", "failed"], "skipped"),
    ("all_done", ["skipped", "failed"], "run"),
    ("one_success", ["skipped", "ok"], "run"),
    ("one_success", ["skipped", "skipped"], "skipped"),
    ("one_success", ["failed", "skipped"], "upstream_failed"),
    ("one_failed", ["ok", "failed"], "run"),
    ("one_failed", ["ok", "skipped"], "skipped"),
    ("one_done", ["failed", "skipped"], "run"),
    ("one_done", ["skipped", "skipped"], "skipped"),
    ("none_failed", ["ok", "skipped"], "run"),
    ("none_failed", ["ok", "failed"], "upstream_failed"),
    ("none_failed_min_one_success", ["ok", "skipped"], "run"),
    ("none_failed_min_one_success", ["skipped", "skipped"], "skipped"),
    ("none_failed_min_one_success", ["failed"], "upstream_failed"),
    ("none_skipped", ["ok", "failed"], "run"),
    ("none_skipped", ["ok", "skipped"], "skipped"),
    ("all_skipped", ["skipped", "skipped"], "run"),
    ("all_skipped", ["ok", "skipped"], "skipped"),
    ("all_done_min_one_success", ["ok", "failed"], "run"),
    ("all_done_min_one_success", ["failed", "failed"], "upstream_failed"),
    ("all_done_min_one_success", ["ok", "skipped"], "skipped"),  # скипнутые не «done»
    ("always", ["failed", "skipped"], "run"),
]


@pytest.mark.parametrize(("rule", "ups", "expected"), CASES)
def test_trigger_rule_table(rule: str, ups: list[str], expected: str) -> None:
    assert trigger_rule_decide(rule, ups) == expected


def test_trigger_rule_unknown_is_loud() -> None:
    with pytest.raises(ValueError, match="неизвестный trigger_rule"):
        trigger_rule_decide("all_sucess", ["ok"])  # опечатка


def test_branch_targets_parsing() -> None:
    class FakeTask:
        task_id = "route"
        downstream_task_ids = {"a", "b"}

    assert branch_targets(FakeTask(), "a") == (["a"], ["b"])
    assert branch_targets(FakeTask(), None) == ([], ["a", "b"])  # None → все skipped
    assert branch_targets(FakeTask(), ["a", "b"]) == (["a", "b"], [])
    with pytest.raises(ValueError, match="downstream-задача"):
        branch_targets(FakeTask(), "c")


# --- прогоны в subprocess ------------------------------------------------------


def test_branch_picks_skips_and_insert_rows(tmp_path: Path) -> None:
    dags, fx = _setup(tmp_path)
    run = run_dag(dags / "dags.py", "branch_etl", fixtures_path=fx)
    assert run.statuses == {
        "extract": "ok",
        "route": "ok",
        "load_events": "ok",
        "load_empty": "skipped",  # не выбрана брэнчем
        "report": "ok",
    }
    # insert_rows записал по-настоящему, report прочитал обратно
    assert "таблица содержит 1" in run.values["report"]


def test_cascade_upstream_failed_and_always(tmp_path: Path) -> None:
    dags, fx = _setup(tmp_path)
    run = run_dag(dags / "dags.py", "cascade", fixtures_path=fx)
    assert run.statuses == {
        "t1": "failed",
        "t2": "upstream_failed",  # all_success за упавшим
        "t3": "upstream_failed",  # none_failed: upstream_failed = фейл
        "t4": "ok",  # always
    }
    assert "нарочно" in run.failures["t1"]


def test_branch_none_skips_everything(tmp_path: Path) -> None:
    dags, fx = _setup(tmp_path)
    run = run_dag(dags / "dags.py", "branch_none", fixtures_path=fx)
    assert run.statuses == {"route": "ok", "a": "skipped", "b": "skipped"}


def test_branch_wrong_task_is_loud(tmp_path: Path) -> None:
    dags, fx = _setup(tmp_path)
    run = run_dag(dags / "dags.py", "branch_wrong", fixtures_path=fx)
    assert run.statuses["route"] == "failed"
    assert "downstream-задача" in run.failures["route"]


def test_insert_rows_on_fake_fixture_is_loud(tmp_path: Path) -> None:
    dags, fx = _setup(tmp_path)
    run = run_dag(dags / "dags.py", "insert_fake", fixtures_path=fx)
    assert run.statuses["ins"] == "failed"
    assert "Fake-фикстура" in run.failures["ins"]


def test_run_cli_upstream_failed_fails_ci(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """upstream_failed в CI — это провал (exit 1), а не мнимый успех (§6.21)."""
    dags, fx = _setup(tmp_path)
    code = main(
        [
            "run",
            "--dags-folder",
            str(dags),
            "--dag-id",
            "cascade",
            "--fixtures",
            str(fx),
            "--format",
            "junit",
        ],
    )
    out = capsys.readouterr().out
    assert code == 1
    assert 'type="UPSTREAM_FAILED"' in out
    assert 'failures="3"' in out  # t1 (фейл) + t2/t3 (upstream_failed), t4 — ok
