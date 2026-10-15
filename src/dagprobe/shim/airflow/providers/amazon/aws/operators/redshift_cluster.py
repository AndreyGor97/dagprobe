"""Shim ``providers.amazon.aws.operators.redshift_cluster`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class RedshiftCreateClusterOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class RedshiftDeleteClusterOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["RedshiftCreateClusterOperator", "RedshiftDeleteClusterOperator"]
