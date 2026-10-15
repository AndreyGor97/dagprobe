"""Shim ``providers.standard.sensors.external_task``: ExternalTaskSensor/Marker.

Структура графа + хранение параметров; ожидание чужого DAG — рантайм Airflow
(в прогоне DagProbe задача уходит в ``skipped``, как прочие не-Python операторы).
"""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator
from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class ExternalTaskSensor(BaseSensorOperator):
    """``poke`` шима кидает loud-ошибку: чужой метастор недоступен."""

    template_fields = ("external_dag_id", "external_task_id", "external_task_ids")

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: str | None = None,
        external_task_ids: list[str] | None = None,
        allowed_states: list[str] | None = None,
        failed_states: list[str] | None = None,
        execution_delta: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.external_task_ids = external_task_ids or (
            [external_task_id] if external_task_id else []
        )
        self.allowed_states = allowed_states or ["success"]
        self.failed_states = failed_states or []
        self.execution_delta = execution_delta

    def poke(self, context: dict[str, Any]) -> bool:
        msg = (
            f"ExternalTaskSensor {self.task_id!r}: ожидание DAG "
            f"{self.external_dag_id!r} — рантайм Airflow, шим не эмулирует"
        )
        raise NotImplementedError(msg)


class ExternalTaskMarker(BaseOperator):
    """Маркер очистки Zombied-задач родительского DAG — в графе обычный узел."""

    def __init__(self, *, external_task_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.external_task_id = external_task_id
