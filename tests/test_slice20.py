"""Срез 20: _success-ключи контекста (закрытие легаси-контекста 2.x).

Зонды против живых движков (первый прогон, без истории dag-run):
- живой 2.10.5 (.scratch/probe20.py): все 5 ключей — prev_data_interval_*_success,
  prev_start_date_success, prev_end_date_success, prev_execution_date_success —
  None, jinja рендерит 'None'; с историей — значения из прошлого успешного
  прогона (включая wall-clock prev_end_date_success) — движком не моделируется.
- живой 3.3.2 (.scratch/probe20b.py, probe20b2.py): в контексте 4 ключа-прокси
  (без prev_execution_date_success), str() → None, рендер 'None';
  {{ prev_execution_date_success }} — громкий UndefinedError, задача failed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from dagprobe.shim import _templates
from dagprobe.task_test import run_dag

LD = datetime(2026, 1, 1, tzinfo=UTC)

SUCCESS_KEYS = (
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_start_date_success",
    "prev_end_date_success",
)


def _task(schedule: object) -> SimpleNamespace:
    return SimpleNamespace(
        task_id="t", params={}, dag=SimpleNamespace(params={}, schedule=schedule)
    )


def test_success_keys_none_both_profiles() -> None:
    for profile in ("2.10", "3.3"):
        ctx = _templates.build_context(_task("@daily"), LD, profile)
        for key in SUCCESS_KEYS:
            assert ctx[key] is None, (profile, key)


def test_success_keys_none_without_schedule() -> None:
    # None-значение не зависит от расписания: история всё равно пуста
    ctx = _templates.build_context(_task(None), LD, "2.10")
    assert ctx["prev_execution_date_success"] is None
    for key in SUCCESS_KEYS:
        assert ctx[key] is None


SUCCESS_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def echo(value):
    return value


with DAG("success_keys", schedule="@daily"):
    PythonOperator(
        task_id="render", python_callable=echo,
        op_kwargs={"value": "|".join([
            "{{ prev_data_interval_start_success }}",
            "{{ prev_data_interval_end_success }}",
            "{{ prev_start_date_success }}",
            "{{ prev_end_date_success }}",
            "{{ prev_execution_date_success }}",
        ])},
    )
"""


def test_profile_v3_renders_prev_execution_date_success_loud(tmp_path: object) -> None:
    file = tmp_path / "success_keys.py"  # type: ignore[operator]
    file.write_text(SUCCESS_DAG, encoding="utf-8")  # type: ignore[operator]
    run = run_dag(file, "success_keys")  # type: ignore[arg-type]
    assert run.statuses["render"] == "failed"
    assert "'prev_execution_date_success' is undefined" in run.failures["render"]


def test_profile_v2_renders_success_keys_none(tmp_path: object) -> None:
    file = tmp_path / "success_keys.py"  # type: ignore[operator]
    file.write_text(SUCCESS_DAG, encoding="utf-8")  # type: ignore[operator]
    run = run_dag(file, "success_keys", profile="2.10")  # type: ignore[arg-type]
    assert run.statuses["render"] == "ok"
    assert run.values["render"] == "'None|None|None|None|None'"
