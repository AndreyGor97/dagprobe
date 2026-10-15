"""Shim ``providers.amazon.aws.sensors.eks``."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class EksClusterStateSensor(BaseSensorOperator):
    """Стаб: покет — рантайм."""


class EksNodegroupStateSensor(BaseSensorOperator):
    """Стаб: покет — рантайм."""


__all__ = ["EksClusterStateSensor", "EksNodegroupStateSensor"]
