"""Shim 2.x-пути: `dummy_operator` — реэкспорт."""

from dagprobe.shim.airflow.operators.dummy import DummyOperator

__all__ = ["DummyOperator"]
