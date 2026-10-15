"""Shim 2.x-пути: ``airflow.models.baseoperator`` — BaseOperator / Operator."""

from dagprobe.shim._core import BaseOperator

Operator = BaseOperator

__all__ = ["BaseOperator", "Operator"]
