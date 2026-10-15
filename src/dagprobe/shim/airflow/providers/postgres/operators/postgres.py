"""Shim ``providers.postgres.operators.postgres``: PostgresOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class PostgresOperator(BaseOperator):
    """Стаб: sql — шаблонное поле; исполнение — рантайм Airflow/БД."""

    template_fields = ("sql",)
