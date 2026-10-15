"""Shim 2.x-пути: `time_delta` — реэкспорт."""

from dagprobe.shim.airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

__all__ = ["TimeDeltaSensor"]
