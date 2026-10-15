"""Shim ``airflow.utils.session.provide_session`` — passthrough (сессии БД нет)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def provide_session(func: Callable[..., Any]) -> Callable[..., Any]:
    """В shim-режиме сессии нет: декоратор ничего не добавляет."""
    return func


def create_session() -> Any:
    raise NotImplementedError("dagprobe shim: DB-сессии Airflow не эмулируются")
