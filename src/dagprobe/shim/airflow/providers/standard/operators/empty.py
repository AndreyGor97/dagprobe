"""Shim: ``airflow.providers.standard.operators.empty`` — ``EmptyOperator``."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmptyOperator(BaseOperator):
    """Shim ``EmptyOperator`` (бывший ``DummyOperator``)."""
