"""Shim 2.x-пути: `date_time` — реэкспорт."""

from dagprobe.shim.airflow.providers.standard.sensors.date_time import DateTimeSensor

__all__ = ["DateTimeSensor"]
