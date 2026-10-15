"""Shim ``providers.amazon.aws.operators.emr_add_steps`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmrAddStepsOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["EmrAddStepsOperator"]
