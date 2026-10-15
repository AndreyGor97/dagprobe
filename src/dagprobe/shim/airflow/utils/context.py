"""Shim ``airflow.utils.context``: Context — словарь контекста рантайма."""

from __future__ import annotations

from typing import Any


class Context(dict[str, Any]):
    """Ключи как в Airflow (``dag_run``, ``task_instance``), значения — рантайм."""
