"""Shim ``providers.amazon.aws.operators.sagemaker`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class SageMakerProcessingOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["SageMakerProcessingOperator"]
