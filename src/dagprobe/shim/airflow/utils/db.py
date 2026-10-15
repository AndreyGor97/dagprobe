"""Shim ``airflow.utils.db`` — только имена; операции с метастором не эмулируются."""

from __future__ import annotations

from typing import Any


def create_session() -> Any:
    raise NotImplementedError("dagprobe shim: DB-сессии Airflow не эмулируются")
