"""Shim ``providers.opensearch.hooks.opensearch``: OpenSearchHook."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class OpenSearchHook(BaseHook):
    """Стаб: conn_id храним; запросы OpenSearch — рантайм Airflow."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.opensearch_conn_id = kwargs.get("opensearch_conn_id")
