"""Shim 2.x-пути ``airflow.datasets`` — Dataset (в 3.x — алиас Asset)."""

from dagprobe.shim._core import Dataset

__all__ = ["Dataset"]
