"""Shim ``providers.apache.hive.operators.hive``: HiveOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class HiveOperator(BaseOperator):
    """Стаб: hql/запрос — атрибуты; исполнение Hive — рантайм Airflow."""

    template_fields = ("hql", "hive_cli_conn_id")
