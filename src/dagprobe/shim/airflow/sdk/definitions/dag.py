"""Shim ``airflow.sdk.definitions.dag``: DAG."""

from dagprobe.shim._core import DAG

__all__ = ["DAG"]
