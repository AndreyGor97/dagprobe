"""Shim ``providers.http.sensors.http``: HttpSensor."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class HttpSensor(BaseSensorOperator):
    """Стаб: endpoint — шаблонное поле; покет — рантайм."""

    template_fields = ("endpoint",)
