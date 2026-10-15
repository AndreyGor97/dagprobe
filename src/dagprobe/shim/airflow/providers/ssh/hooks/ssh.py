"""Shim ``providers.ssh.hooks.ssh``: SSHHook."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class SSHHook(BaseHook):
    """Стаб: conn_id храним; SSH-сессии в шиме нет."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.ssh_conn_id = kwargs.get("ssh_conn_id")
