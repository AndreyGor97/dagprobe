"""Shim ``providers.standard.sensors.filesystem``: FileSensor."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class FileSensor(BaseSensorOperator):
    """Стаб: filepath — шаблонное поле; проверка файла — рантайм."""

    template_fields = ("filepath",)
