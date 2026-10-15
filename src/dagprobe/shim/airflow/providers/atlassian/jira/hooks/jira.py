"""Shim ``providers.atlassian.jira.hooks.jira``: JiraHook."""

from __future__ import annotations

from typing import Any

from dagprobe.shim.airflow.sdk.bases.hook import BaseHook


class JiraHook(BaseHook):
    """Стаб: conn_id храним; обращение к Jira — рантайм Airflow."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.jira_conn_id = kwargs.get("jira_conn_id")
