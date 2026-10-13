"""Jinja-рендер ``template_fields`` в шиме.

Семантика — по ``airflow.sdk.execution_time.task_runner`` / ``templater``
(airflow 3.3.2): ``SandboxedEnvironment(cache_size=0)`` с фильтрами
``ds``/``ds_nodash``/``ts``/``ts_nodash``/``ts_nodash_with_tz``, контекст-словарь
от ``logical_date`` и ленивый ``macros``.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import Any

import jinja2.sandbox
from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment


def ds_add(ds: str, days: int) -> str:
    """Сдвиг даты ``YYYY-MM-DD`` на ``days`` дней (семантика macros.ds_add)."""
    return (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")


def ds_format(ds: str, input_format: str, output_format: str) -> str:
    """Переформатирование даты (семантика macros.ds_format)."""
    return datetime.strptime(ds, input_format).strftime(output_format)


def _ds_filter(value: Any) -> str | None:
    return None if value is None else value.strftime("%Y-%m-%d")


def _ds_nodash_filter(value: Any) -> str | None:
    return None if value is None else value.strftime("%Y%m%d")


def _ts_filter(value: Any) -> str | None:
    return None if value is None else value.isoformat()


def _ts_nodash_filter(value: Any) -> str | None:
    return None if value is None else value.strftime("%Y%m%dT%H%M%S")


def _ts_nodash_with_tz_filter(value: Any) -> str | None:
    return None if value is None else value.isoformat().replace("-", "").replace(":", "")


FILTERS = {
    "ds": _ds_filter,
    "ds_nodash": _ds_nodash_filter,
    "ts": _ts_filter,
    "ts_nodash": _ts_nodash_filter,
    "ts_nodash_with_tz": _ts_nodash_with_tz_filter,
}


class _AirflowEnvironment(SandboxedEnvironment):
    """Как ``airflow...templater.SandboxedEnvironment``: фильтры + доступ к ``_attr``."""

    def __init__(self, **kwargs: Any) -> None:
        # StrictUndefined — как airflow sdk templater (templater.py:381):
        # отсутствующий ключ контекста — громкий UndefinedError, не тихий рендер
        # в пусто (зонд Срез 18 против живого 3.3.2: {{ prev_ds }} → задача failed)
        super().__init__(undefined=StrictUndefined, **kwargs)
        self.filters.update(FILTERS)

    def is_safe_attribute(self, obj: Any, attr: str, value: Any) -> bool:
        return not jinja2.sandbox.is_internal_attribute(obj, attr)


class Macros:
    """Ленивые макросы. Непокрытое имя — громкий ``AttributeError``, не тихий None.

    ponytail: только макросы, нужные эталонным DAG + дешёвый stdlib-проход;
    остальное (dateutil, yaml, datetime_diff_for_humans) — добавлять по мере
    появления в пользовательских DAG.
    """

    _names = {
        "ds_add": ds_add,
        "ds_format": ds_format,
        "datetime": datetime,
        "timedelta": timedelta,
        "uuid": uuid,
        "random": random,
        "time": time,
        "json": json,
    }

    def __getattr__(self, item: str) -> Any:
        try:
            return self._names[item]
        except KeyError:
            msg = f"macro macros.{item} is not implemented by the shim — add it to Macros._names"
            raise AttributeError(msg) from None


class _Ti:
    """``ti``/``task_instance`` в контексте: ``xcom_pull`` с версионной семантикой.

    Срез 9 (G2 #1): ``task_ids=None`` — в 2.x последний XCom всего dag-run,
    в 3.x — только текущая задача (типично None). Списковый/строковый ``task_ids``
    — одинаково в обеих версиях: список → список значений, строка → значение
    (поправка Среза 17, 13.10: живой A/B против 3.3.2 опроверг #49540 —
    ``['t']`` из одного элемента тоже даёт список, а не само значение).
    Отсутствующий XCom — None (как в Airflow), не громкая ошибка: тишину тут
    ловит сам ``audit``, сравнивая профили.
    """

    def __init__(self, task: Any, profile: str) -> None:
        self._task_id: str = task.task_id
        self._profile = profile

    @property
    def task_id(self) -> str:
        return self._task_id

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value", **_: Any) -> Any:
        from dagprobe.shim.xcom import xcom as store

        def one(tid: str) -> Any:
            try:
                return store.get(tid, key)
            except KeyError:
                return None

        if isinstance(task_ids, str):
            return one(task_ids)
        if task_ids is None:
            if self._profile == "2.10":
                return store.latest(key)  # весь dag-run, последний по записи
            return one(self._task_id)  # 3.x: только текущая задача
        return [one(t) for t in task_ids]


_cv: ContextVar[dict[str, Any] | None] = ContextVar("dagprobe_context", default=None)


@contextmanager
def current_context(ctx: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Поднять контекст задачи на время исполнения callable (для get_current_context)."""
    token = _cv.set(ctx)
    try:
        yield ctx
    finally:
        _cv.reset(token)


def get_current_context() -> dict[str, Any]:
    """Как ``airflow.operators.python.get_current_context``; вне прогона — громко."""
    ctx = _cv.get()
    if ctx is None:
        msg = "context is only available inside task execution of a run"
        raise RuntimeError(msg)
    return ctx


def build_context(task: Any, logical_date: datetime, profile: str = "3.3") -> dict[str, Any]:
    """Контекст рендера: ключи от ``logical_date`` + ``params``/``macros``.

    Профиль версии (Срез 8): ``"3.3"`` — текущий контекст Airflow 3;
    ``"2.10"`` добавляет легаси-ключи из 2.x — ``execution_date``,
    ``tomorrow_ds``/``yesterday_ds`` (и nododash-варианты), а также
    ``prev_ds``/``next_ds`` (Срез 18) через timetable-эмуляцию расписания
    (``_timetable.prev_next``): расписание известно → честный расчёт,
    отсутствует (``None``/``@once``) → пустые строки (как 2.x), неизвестный
    вид → громкий ``ValueError``. В профиле ``"3.3"`` этих ключей нет —
    рендер ``{{ prev_ds }}`` громко падает UndefinedError (StrictUndefined),
    как живой 3.3.2 (зонд Срез 18).

    ponytail: ``var``/``conn`` не входят — это L1-фикстуры (Срез 2+); шаблон с
    ``{{ var.value.x }}`` падает громкой ошибкой Jinja, а не тихо рендерится.
    """
    ds = logical_date.strftime("%Y-%m-%d")
    ts = logical_date.isoformat()
    ctx: dict[str, Any] = {
        "ds": ds,
        "ds_nodash": ds.replace("-", ""),
        "ts": ts,
        "ts_nodash": logical_date.strftime("%Y%m%dT%H%M%S"),
        "ts_nodash_with_tz": ts.replace("-", "").replace(":", ""),
        "logical_date": logical_date,
        "params": {**getattr(task.dag, "params", {}), **task.params},
        "macros": Macros(),
        "task": task,
        "dag": task.dag,
        "run_id": "dagprobe",
        "ti": _Ti(task, profile),
        "task_instance": _Ti(task, profile),
    }
    if profile == "2.10":
        from dagprobe.shim import _timetable

        prev_dt, next_dt = _timetable.prev_next(logical_date, getattr(task.dag, "schedule", None))
        yesterday = ds_add(ds, -1)
        tomorrow = ds_add(ds, 1)
        ctx.update(
            {
                "execution_date": logical_date.replace(tzinfo=None),  # в 2.x — naive UTC
                "yesterday_ds": yesterday,
                "yesterday_ds_nodash": yesterday.replace("-", ""),
                "tomorrow_ds": tomorrow,
                "tomorrow_ds_nodash": tomorrow.replace("-", ""),
                # легаси prev/next (2.x): None при отсутствии расписания (jinja → 'None')
                "prev_execution_date": prev_dt.replace(tzinfo=None) if prev_dt else None,
                "next_execution_date": next_dt.replace(tzinfo=None) if next_dt else None,
                "prev_ds": prev_dt.strftime("%Y-%m-%d") if prev_dt else None,
                "prev_ds_nodash": prev_dt.strftime("%Y%m%d") if prev_dt else None,
                "next_ds": next_dt.strftime("%Y-%m-%d") if next_dt else None,
                "next_ds_nodash": next_dt.strftime("%Y%m%d") if next_dt else None,
            }
        )
    elif profile != "3.3":
        msg = f"unknown profile {profile!r} (known: 2.10, 3.3)"
        raise ValueError(msg)
    return ctx


def _render(value: Any, env: _AirflowEnvironment, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return env.from_string(value).render(context)
    if isinstance(value, dict):
        return {k: _render(v, env, context) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_render(v, env, context) for v in value)
    return value


def render_template_fields(task: Any, context: dict[str, Any]) -> None:
    """Отрендерить каждый ``template_fields`` на месте (как operator.render_template_fields)."""
    env = _AirflowEnvironment(cache_size=0)
    for field in getattr(task, "template_fields", ()):
        setattr(task, field, _render(getattr(task, field, None), env, context))
