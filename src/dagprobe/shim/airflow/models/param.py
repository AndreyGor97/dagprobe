"""Shim ``airflow.models.param``: Param/ParamsDict/DagParam (parse-time хранение)."""

from __future__ import annotations

from typing import Any


class Param:
    """Стаб ``Param``: default + description (валидация схемы не эмулируется)."""

    def __init__(self, default: Any = None, description: str | None = None, **kwargs: Any) -> None:
        self.default = default
        self.description = description
        self.kwargs = kwargs


class ParamsDict(dict[str, Any]):
    """dict без валидации: parse-time достаточно хранения."""


class DagParam:
    """Стаб ``dag.param``: имя + default; resolve() — рантайм Airflow."""

    def __init__(self, dag: Any, name: str, default: Any = None) -> None:
        self._dag = dag
        self._name = name
        self._default = default

    def resolve(self, context: dict[str, Any] | None = None) -> Any:
        return self._default
