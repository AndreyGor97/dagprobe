"""Shim ``airflow.operators.branch``: BaseBranchOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class BaseBranchOperator(BaseOperator):
    """Стаб: выбор ветки ``choose_branch`` — рантайм Airflow."""

    def choose_branch(self, context: object) -> object:
        raise NotImplementedError("dagprobe shim: выбор ветки исполняется движком Airflow")
