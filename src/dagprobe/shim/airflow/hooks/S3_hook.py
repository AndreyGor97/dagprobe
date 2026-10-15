"""Shim 2.x-пути: ``airflow.hooks.s3_hook`` — S3Hook."""

from dagprobe.shim.airflow.providers.amazon.aws.hooks.s3 import S3Hook

__all__ = ["S3Hook"]
