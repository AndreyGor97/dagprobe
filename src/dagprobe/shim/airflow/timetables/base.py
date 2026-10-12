"""Shim: ``airflow.timetables.base`` — контракты расписания (DagRunInfo, DataInterval, Timetable).

Гипотеза 5 (файл 03): классы-заглушки достаточны, чтобы реальная логика кастомного
Timetable (``workday.py``) исполнялась как есть — это и есть поведенческая проверка.
"""

from __future__ import annotations

from typing import Any


class DataInterval:
    """Интервал данных прогона: ``start``/``end`` (pendulum DateTime)."""

    def __init__(self, *, start: Any, end: Any) -> None:
        self.start = start
        self.end = end


class DagRunInfo:
    """Когда и с каким интервалом создать следующий прогон."""

    def __init__(self, *, logical_date: Any, data_interval: DataInterval) -> None:
        self.logical_date = logical_date
        self.data_interval = data_interval

    @classmethod
    def interval(cls, *, start: Any, end: Any) -> DagRunInfo:
        return cls(logical_date=start, data_interval=DataInterval(start=start, end=end))


class Timetable:
    """База кастомных расписаний; реальную логику несёт подкласс пользователя."""

    def serialize(self) -> dict[str, Any]:  # pragma: no cover - контракт Airflow
        raise NotImplementedError


class TimeRestriction:
    """Ограничения планирования (earliest/latest/catchup) — только для типа."""

    def __init__(self, *, earliest: Any = None, latest: Any = None, catchup: bool = True) -> None:
        self.earliest = earliest
        self.latest = latest
        self.catchup = catchup
