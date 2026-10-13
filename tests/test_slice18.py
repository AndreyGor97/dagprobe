"""Срез 18: prev_ds/next_ds через timetable-эмуляцию.

Зонд против живого 3.3.2 (.scratch/probe18.py): ключей prev_*/next_* в
контексте 3.x нет при любом расписании, а ``{{ prev_ds }}`` — громкий
UndefinedError (StrictUndefined), задача failed на рендере. Значит: профиль
3.3 громко падает (находка аудита), профиль 2.10 честно считает prev/next
из расписания (сверка расчёта с croniter — spike/check_timetable.py) или
отдаёт None при отсутствии расписания. Семантика сверена с живым 2.10.5
(.scratch/probe210.py): next = data_interval_end (последний тик ≤ logical_date),
prev = previous_schedule (строго до), None/@once → None (jinja → 'None').
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dagprobe.shim import _templates, _timetable
from dagprobe.task_test import run_dag

LD = datetime(2026, 1, 1, tzinfo=UTC)


def _task(schedule: object) -> SimpleNamespace:
    return SimpleNamespace(
        task_id="t", params={}, dag=SimpleNamespace(params={}, schedule=schedule)
    )


def test_prev_next_none_and_once() -> None:
    assert _timetable.prev_next(LD, None) == (None, None)
    assert _timetable.prev_next(LD, "@once") == (None, None)


def test_prev_next_timedelta() -> None:
    from datetime import timedelta

    # живой 2.10: next = data_interval_end = сам logical_date
    assert _timetable.prev_next(LD, timedelta(hours=6)) == (
        LD.replace(tzinfo=None) - timedelta(hours=6),
        LD.replace(tzinfo=None),
    )


def test_profile_v2_daily() -> None:
    ctx = _templates.build_context(_task("@daily"), LD, "2.10")
    assert ctx["prev_ds"] == "2025-12-31"
    assert ctx["prev_ds_nodash"] == "20251231"
    assert ctx["next_ds"] == "2026-01-01"  # на сетке: next = data_interval_end = LD
    assert ctx["prev_execution_date"] == datetime(2025, 12, 31)  # naive, как 2.x


def test_profile_v2_cron_offset() -> None:
    ctx = _templates.build_context(_task("30 4 * * *"), LD, "2.10")
    # LD вне сетки: next = data_interval_end = последний тик ≤ LD = prev
    assert ctx["prev_execution_date"] == datetime(2025, 12, 31, 4, 30)
    assert ctx["next_execution_date"] == datetime(2025, 12, 31, 4, 30)


def test_profile_v2_no_schedule_none() -> None:
    ctx = _templates.build_context(_task(None), LD, "2.10")
    assert ctx["prev_ds"] is None and ctx["next_ds"] is None
    assert ctx["prev_execution_date"] is None and ctx["next_execution_date"] is None


def test_unknown_schedule_is_loud() -> None:
    with pytest.raises(ValueError, match="can only emulate"):
        _templates.build_context(_task(lambda *_: None), LD, "2.10")


def test_cron_garbage_field_is_loud() -> None:
    with pytest.raises(ValueError, match="not supported"):
        _timetable.prev_next(LD, "0 0 * * FUNDAY")


PREV_DS_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def echo(value):
    return value


with DAG("prev_ds", schedule="@daily"):
    PythonOperator(
        task_id="render", python_callable=echo,
        op_kwargs={"value": "{{ prev_ds }}|{{ next_ds }}"},
    )
"""


def test_profile_v3_renders_prev_ds_loud(tmp_path: object) -> None:
    file = tmp_path / "prev_ds.py"  # type: ignore[operator]
    file.write_text(PREV_DS_DAG, encoding="utf-8")  # type: ignore[operator]
    run = run_dag(file, "prev_ds")  # type: ignore[arg-type]
    assert run.statuses["render"] == "failed"
    assert "'prev_ds' is undefined" in run.failures["render"]


def test_profile_v2_renders_prev_ds(tmp_path: object) -> None:
    file = tmp_path / "prev_ds.py"  # type: ignore[operator]
    file.write_text(PREV_DS_DAG, encoding="utf-8")  # type: ignore[operator]
    run = run_dag(file, "prev_ds", profile="2.10")  # type: ignore[arg-type]
    assert run.statuses["render"] == "ok"
    # логическая дата прогона — из LOGICAL_DATE движка (2026-09-29, на сетке @daily)
    assert run.values["render"] == "'2026-09-28|2026-09-29'"
