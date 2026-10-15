"""Shim ``providers.postgres.hooks.postgres``: PostgresHook."""

from __future__ import annotations

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class PostgresHook(BaseHook):
    """Стаб: conn_id храним; SQL — через Fake/Emulated маршрутизацию Срезов 3–4."""

    def __init__(self, postgres_conn_id: str = "postgres_default", **kwargs: object) -> None:
        self.conn_id = postgres_conn_id
