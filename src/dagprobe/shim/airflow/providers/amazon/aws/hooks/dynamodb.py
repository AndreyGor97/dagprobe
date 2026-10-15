"""Shim ``providers.amazon.aws.hooks.dynamodb``."""

from __future__ import annotations

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class AwsDynamoDBHook(BaseHook):
    """Стаб: conn_id храним; обращение к AWS — рантайм."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        pass


__all__ = ["AwsDynamoDBHook"]
