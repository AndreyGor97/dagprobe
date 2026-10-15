"""Shim ``providers.amazon.aws.operators.emr`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmrServerlessCreateApplicationOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EmrServerlessDeleteApplicationOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EmrServerlessStartJobOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = [
    "EmrServerlessCreateApplicationOperator",
    "EmrServerlessDeleteApplicationOperator",
    "EmrServerlessStartJobOperator",
]
