"""Shim ``providers.amazon.aws.sensors.emr_step``."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class EmrStepSensor(BaseSensorOperator):
    """Стаб: покет — рантайм."""


__all__ = ["EmrStepSensor"]
