"""Shim ``providers.amazon.aws.hooks.s3``: S3Hook (3.x-путь)."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class S3Hook(AwsBaseHook):
    """Стаб: ключи/бакеты — атрибуты; обращение к S3 — рантайм Airflow."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.bucket = kwargs.get("bucket") if "bucket" in kwargs else None
