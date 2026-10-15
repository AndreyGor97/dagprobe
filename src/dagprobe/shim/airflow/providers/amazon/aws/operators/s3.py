"""Shim ``providers.amazon.aws.operators.s3`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class S3CopyObjectOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class S3CreateObjectOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class S3DeleteObjectsOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["S3CopyObjectOperator", "S3CreateObjectOperator", "S3DeleteObjectsOperator"]
