"""Shim ``airflow.sdk.definitions.mappedoperator``: MappedOperator — имя на parse."""

from __future__ import annotations

from typing import Any


class MappedOperator:
    """Стаб: раскрытие маппинга — OperatorPartial (``.partial().expand()``)."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
