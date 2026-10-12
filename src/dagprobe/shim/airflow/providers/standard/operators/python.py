"""Shim: ``airflow.providers.standard.operators.python.PythonOperator`` (Срез 3).

Task Test (§6.5) исполняет ``python_callable`` — тело пользовательской задачи.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dagprobe.shim._core import BaseOperator


class PythonOperator(BaseOperator):
    """Shim ``PythonOperator``: python_callable + op_args/op_kwargs."""

    template_fields = ("templates_dict", "op_args", "op_kwargs")

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: tuple[Any, ...] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            task_id=kwargs.pop("task_id"), **kwargs
        )  # trigger_rule и пр. — как в Airflow
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}


class BranchPythonOperator(PythonOperator):
    """Shim ``BranchPythonOperator``: callable возвращает task_id(ы) выбранных веток.

    Сам класс ничего не добавляет: выбор веток разбирает движок прогона
    (``task_test.branch_targets``) по семантике BranchMixIn (airflow 3.3.2).
    ``@task.branch`` — отложено (Срез 7, «Отложено» в BACKLOG).
    """


def get_current_context() -> dict[str, Any]:
    """Shim ``get_current_context`` (Срез 9): ``ti`` и прочие ключи контекста прогона."""
    from dagprobe.shim._templates import get_current_context as _get

    return _get()
