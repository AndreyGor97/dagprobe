"""Shim 2.x-пути: ``airflow.operators.python`` → providers.standard (как в Airflow 3)."""

from dagprobe.shim.airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
    get_current_context,
)

__all__ = ["BranchPythonOperator", "PythonOperator", "ShortCircuitOperator", "get_current_context"]
