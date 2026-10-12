"""Shim: ``airflow.providers.standard.operators.bash`` — ``BashOperator``."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class BashOperator(BaseOperator):
    """Shim ``BashOperator``: ``bash_command`` — Jinja-шаблон (template field)."""

    template_fields = ("bash_command",)
