"""Shim ``airflow.sdk.definitions`` — пространство имён 3.x."""

from dagprobe.shim.airflow.sdk.definitions.asset import Asset
from dagprobe.shim.airflow.sdk.definitions.dag import DAG

__all__ = ["Asset", "DAG"]
