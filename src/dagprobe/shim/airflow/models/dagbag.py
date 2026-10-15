"""Shim ``DagBag``: реестр DAG текущего прогона, без сканирования папки.

Внешние тесты (``tests/dags/test_dag_example.py``) создают DagBag на parse —
отдаём то, что уже спарсено шимом; ленивый импорт папки не эмулируется.
"""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import DAG, all_dags


class DagBag:
    """Стаб: ``dags`` — снимок реестра шима на момент создания."""

    def __init__(self, dag_folder: str | None = None, **kwargs: Any) -> None:
        self.dag_folder = dag_folder
        self.dags: dict[str, DAG] = dict(all_dags())
        self.import_errors: dict[str, str] = {}

    def get_dag(self, dag_id: str) -> DAG | None:
        return self.dags.get(dag_id)
