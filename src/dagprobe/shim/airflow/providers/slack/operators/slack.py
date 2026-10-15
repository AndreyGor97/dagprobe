"""Shim ``providers.slack.operators.slack``: SlackAPIPostOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class SlackAPIPostOperator(BaseOperator):
    """Стаб: канал/текст — шаблонные поля; отправка — рантайм Airflow."""

    template_fields = ("channel", "text")
