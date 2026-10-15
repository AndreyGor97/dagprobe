"""Shim 2.x-пути: `hive_operator` — реэкспорт."""

from dagprobe.shim.airflow.providers.apache.hive.operators.hive import HiveOperator

__all__ = ["HiveOperator"]
