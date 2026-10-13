"""Shim: ``airflow.plugins_manager`` — регистрация плагинов (для example DAG с Timetable)."""

from __future__ import annotations

from typing import Any


class AirflowPlugin:
    """База плагинов: подкласс объявляет ``name`` и списки расширений (у нас — timetables)."""

    name: str = ""
    timetables: list[type] = []


__all__ = ["AirflowPlugin"]


def __getattr__(name: str) -> Any:
    msg = f"shim airflow.plugins_manager: {name!r} is not implemented yet"
    raise AttributeError(msg)
