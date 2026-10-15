"""Shim ``providers.amazon.aws.sensors.s3_key``: S3KeySensor."""

from __future__ import annotations

from dagprobe.shim.airflow.providers.standard.sensors.base import BaseSensorOperator


class S3KeySensor(BaseSensorOperator):
    """Стаб: bucket_key — шаблонное поле; проверка ключа — рантайм."""

    template_fields = ("bucket_key", "bucket_name")


__all__ = ["S3KeySensor"]
