"""Shim ``providers.amazon.aws.hooks.base_aws``: AwsBaseHook."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class AwsBaseHook(BaseHook):
    """Стаб: aws_conn_id храним; сессии boto3 в шиме нет."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.aws_conn_id = kwargs.get("aws_conn_id")
