"""Shim ``providers.databricks.operators.databricks`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class DatabricksRunNowOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["DatabricksRunNowOperator"]
