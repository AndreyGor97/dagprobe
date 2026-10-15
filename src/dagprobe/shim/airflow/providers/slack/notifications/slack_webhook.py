"""Shim ``providers.slack.notifications.slack_webhook``."""

from __future__ import annotations

from typing import Any


class SlackWebhookNotifier:
    """Стаб-нотифаер: аргументы храним, отправка — рантайм Airflow."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def send_slack_webhook_notification(**kwargs: Any) -> SlackWebhookNotifier:
    return SlackWebhookNotifier(**kwargs)
