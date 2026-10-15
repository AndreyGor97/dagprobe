"""Shim 2.x-пути: ``airflow.operators.bash`` → providers.standard."""

from dagprobe.shim.airflow.providers.standard.operators.bash import BashOperator

__all__ = ["BashOperator"]
