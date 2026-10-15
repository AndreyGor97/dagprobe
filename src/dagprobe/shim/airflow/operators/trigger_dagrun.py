"""Shim 2.x-пути: ``airflow.operators.trigger_dagrun.TriggerDagRunOperator``.

Parse-time: валидация ``trigger_dag_id`` и хранение конфов; реальный триггер — рантайм Airflow.
"""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class TriggerDagRunOperator(BaseOperator):
    """Shim ``TriggerDagRunOperator``: trigger_dag_id + conf (2.7 сигнатура)."""

    template_fields = ("trigger_dag_id", "conf")

    def __init__(
        self, *, trigger_dag_id: str, conf: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.conf = conf or {}
