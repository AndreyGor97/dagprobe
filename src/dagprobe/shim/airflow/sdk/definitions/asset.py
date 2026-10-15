"""Shim ``airflow.sdk.definitions.asset``: Asset (шим-класс Dataset)."""

from dagprobe.shim._core import Dataset as Asset

__all__ = ["Asset"]
