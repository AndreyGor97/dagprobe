"""Airflow-зеркало shim'а. ``import airflow`` попадает сюда через DagProbeFinder.

Корень воздуха 3 отдаёт публичные имена лениво через ``__getattr__`` +
``__lazy_imports`` (проверено на 3.3.2). Без этого валидный код 2→3
(``from airflow import DAG``) ложно падал ``IMPORT_FAILURE`` — BACKLOG, найдено.
"""

from __future__ import annotations

import importlib
from typing import Any

# Пока только то, что шим реально умеет; не остальное — падаем громко ниже.
_LAZY: dict[str, tuple[str, str]] = {
    "DAG": ("dagprobe.shim._core", "DAG"),
    "XComArg": ("dagprobe.shim._taskflow", "XComArg"),
}

__all__ = list(_LAZY)


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        module_name, attr = _LAZY[name]
        return getattr(importlib.import_module(module_name), attr)
    msg = f"shim airflow: {name!r} is not implemented yet"
    raise AttributeError(msg)
