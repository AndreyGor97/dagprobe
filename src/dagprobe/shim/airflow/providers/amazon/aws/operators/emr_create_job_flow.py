"""Shim ``providers.amazon.aws.operators.emr_create_job_flow`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmrCreateJobFlowOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["EmrCreateJobFlowOperator"]
