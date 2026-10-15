"""Shim ``airflow.operators.latest_only``: LatestOnlyOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class LatestOnlyOperator(BaseOperator):
    """Стаб: логика «только последний интервал» — рантайм Airflow."""
