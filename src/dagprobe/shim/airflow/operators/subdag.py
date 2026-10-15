"""Shim ``airflow.operators.subdag``: SubDagOperator."""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class SubDagOperator(BaseOperator):
    """Стаб: subdag — атрибут; вложенный DAG парсится как обычный."""

    def __init__(self, *, dag: Any = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.subdag = dag
