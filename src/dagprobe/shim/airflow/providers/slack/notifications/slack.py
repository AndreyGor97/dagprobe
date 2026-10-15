"""Shim ``providers.slack.notifications.slack``: send_slack_notification."""

from __future__ import annotations

from typing import Any


def send_slack_notification(**kwargs: Any) -> dict[str, Any]:
    """Стаб-фабрика нотифаера: аргументы храним, отправка — рантайм Airflow."""
    return kwargs
