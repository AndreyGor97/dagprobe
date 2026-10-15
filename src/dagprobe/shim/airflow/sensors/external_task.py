"""Shim 2.x-пути: ``airflow.sensors.external_task`` — ExternalTaskSensor/Marker."""

from dagprobe.shim.airflow.providers.standard.sensors.external_task import (
    ExternalTaskMarker,
    ExternalTaskSensor,
)

__all__ = ["ExternalTaskMarker", "ExternalTaskSensor"]
