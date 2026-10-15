"""Shim ``providers.http.hooks.http``: HttpHook."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class HttpHook(BaseHook):
    """Стаб: method/http_conn_id храним; HTTP-вызовы — рантайм Airflow."""

    def __init__(
        self, method: str = "GET", http_conn_id: str = "http_default", **kwargs: Any
    ) -> None:
        self.method = method
        self.http_conn_id = http_conn_id

    def run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("dagprobe shim: HTTP-запросы не исполняются")
