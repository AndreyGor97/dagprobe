"""Shim ``airflow.operators.email``: EmailOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EmailOperator(BaseOperator):
    """Стаб: to/subject/html_content — шаблонные поля; отправка — рантайм Airflow."""

    template_fields = ("to", "subject", "html_content")
