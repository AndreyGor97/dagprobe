"""Shim ``providers.amazon.aws.sensors.emr``."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class EmrServerlessJobSensor(BaseSensorOperator):
    """Стаб: покет — рантайм."""


__all__ = ["EmrServerlessJobSensor"]
