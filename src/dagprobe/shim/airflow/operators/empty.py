"""Shim 2.x-пути: ``airflow.operators.empty`` — EmptyOperator (как в 2.4+)."""

from dagprobe.shim.airflow.providers.standard.operators.empty import EmptyOperator

__all__ = ["EmptyOperator"]
