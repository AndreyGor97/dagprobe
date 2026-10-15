"""Shim ``providers.standard.sensors.date_time``: DateTimeSensor."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class DateTimeSensor(BaseSensorOperator):
    """Стаб: target_time — атрибут; ожидание — рантайм."""

    template_fields = ("target_time",)
