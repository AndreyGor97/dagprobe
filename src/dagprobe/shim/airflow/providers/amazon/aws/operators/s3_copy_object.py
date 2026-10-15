"""Shim ``providers.amazon.aws.operators.s3_copy_object`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class S3CopyObjectOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["S3CopyObjectOperator"]
