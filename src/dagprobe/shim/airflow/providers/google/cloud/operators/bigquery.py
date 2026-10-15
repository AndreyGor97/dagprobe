"""Shim ``providers.google.cloud.operators.bigquery`` (по корпусу RealDag2)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class BigQueryDeleteTableOperator(BaseOperator):
    """Стаб: project/dataset/table — шаблонные поля; удаление — рантайм Airflow."""

    template_fields = ("deletion_dataset_table", "project_id")
