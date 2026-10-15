"""Shim 2.x-пути: ``airflow.operators.dummy`` — DummyOperator/EmptyOperator."""

from dagprobe.shim.airflow.providers.standard.operators.empty import EmptyOperator

DummyOperator = EmptyOperator  # в 2.x оба имени жили здесь

__all__ = ["DummyOperator", "EmptyOperator"]
