"""Shim ``providers.standard.sensors.time_delta``: TimeDeltaSensor."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class TimeDeltaSensor(BaseSensorOperator):
    """Стаб: timedelta — атрибут; ожидание — рантайм."""

    template_fields = ("delta",)
