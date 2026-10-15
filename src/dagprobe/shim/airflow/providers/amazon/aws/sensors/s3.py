"""Shim ``providers.amazon.aws.sensors.s3`` — реэкспорт."""

from dagprobe.shim.airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

__all__ = ["S3KeySensor"]
