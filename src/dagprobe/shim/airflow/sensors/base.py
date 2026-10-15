"""Shim 2.x-пути: ``airflow.sensors.base`` — BaseSensorOperator / PokeReturnValue."""

from dagprobe.shim.airflow.providers.standard.sensors.base import (
    BaseSensorOperator,
    PokeReturnValue,
)

__all__ = ["BaseSensorOperator", "PokeReturnValue"]
