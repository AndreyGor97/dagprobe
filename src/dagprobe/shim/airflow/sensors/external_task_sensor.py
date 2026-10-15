"""Shim 2.x-пути: `external_task_sensor` — реэкспорт."""

from dagprobe.shim.airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

__all__ = ["ExternalTaskMarker", "ExternalTaskSensor"]
