"""Shim 2.x-пути: `bash_operator` — реэкспорт."""

from dagprobe.shim.airflow.operators.bash import BashOperator

__all__ = ["BashOperator"]
