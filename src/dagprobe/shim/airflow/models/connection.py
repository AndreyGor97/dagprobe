"""Shim 2.x-пути: ``airflow.models.connection`` — Connection."""

from dagprobe.shim.airflow.sdk.bases.hook import Connection

__all__ = ["Connection"]
