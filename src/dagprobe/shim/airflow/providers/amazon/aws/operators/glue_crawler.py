"""Shim ``providers.amazon.aws.operators.glue_crawler`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class AwsGlueCrawlerOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class GlueCrawlerOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = ["AwsGlueCrawlerOperator", "GlueCrawlerOperator"]
