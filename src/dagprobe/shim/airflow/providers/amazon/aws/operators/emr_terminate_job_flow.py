"""Shim ``providers.amazon.aws.operators.emr_terminate_job_flow`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmrTerminateJobFlowOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["EmrTerminateJobFlowOperator"]
