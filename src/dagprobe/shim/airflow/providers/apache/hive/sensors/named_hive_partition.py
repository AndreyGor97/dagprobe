"""Shim ``providers.apache.hive.sensors.named_hive_partition``."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class NamedHivePartitionSensor(BaseSensorOperator):
    """Стаб: список партиций — атрибут; покет — рантайм."""

    template_fields = ("partition_names",)

    def __init__(self, *, partition_names: list[str] | None = None, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.partition_names = partition_names or []
