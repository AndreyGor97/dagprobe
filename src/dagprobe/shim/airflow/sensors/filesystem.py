"""Shim 2.x-пути: `filesystem` — реэкспорт."""

from dagprobe.shim.airflow.providers.standard.sensors.filesystem import FileSensor

__all__ = ["FileSensor"]
