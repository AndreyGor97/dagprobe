"""Механика shim'а (не входит в зеркало ``airflow.*``): DAG и BaseOperator.

Минимум под эталонные DAG, инкрементально (файл 18, B3–B5). Никакой магии:
DAG — реестр задач + рёбра зависимостей; оператор — узел с ``task_id``,
``upstream/downstream`` множествами и ``>>``/``<<``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from dagprobe.shim._taskflow import TaskGroup
    from dagprobe.shim._taskflow import XComArg as TaskFlow_XComArg

OperatorArg: TypeAlias = "BaseOperator | Iterable[BaseOperator]"  # forward-refs, класс ниже

_current_dag: DAG | None = None
_current_group: TaskGroup | None = None  # noqa: F821 — type из _taskflow, forward-ref


def current_dag() -> DAG | None:
    return _current_dag


_dag_registry: dict[str, DAG] = {}


def current_group() -> TaskGroup | None:  # noqa: F821
    return _current_group


def set_current_group(group: TaskGroup | None) -> None:  # noqa: F821
    global _current_group
    _current_group = group


def all_dags() -> dict[str, DAG]:
    """Все созданные DAG этого прогона (для ``dagprobe list`` и тестов спайка)."""
    return _dag_registry


def reset_registry() -> None:
    _dag_registry.clear()


class BaseOperator:
    """Общий базовый класс операторов: регистрация в DAG + зависимости."""

    task_id: str
    dag: DAG
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]
    template_fields: tuple[str, ...] = ()

    def __init__(self, task_id: str, dag: DAG | None = None, **kwargs: Any) -> None:
        group = current_group()
        if group is not None:
            task_id = f"{group.group_id}.{task_id}"  # как в Airflow: «op.add_1»
        self.task_id = task_id
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        self.params: dict[str, Any] = {}
        for key, value in kwargs.items():
            setattr(self, key, value)  # default_args и явные аргументы — как атрибуты
        target = dag if dag is not None else _current_dag
        if target is None:
            msg = f"Operator {task_id!r} создан вне контекста DAG"
            raise RuntimeError(msg)
        self.dag = target
        target.add_task(self)

    @property
    def output(self) -> TaskFlow_XComArg:
        """``task.output`` — результат задачи как XComArg (как в Airflow)."""
        from dagprobe.shim._taskflow import XComArg

        return XComArg(self)

    # -- зависимости: t1 >> t2 / t1 >> [t2, t3] / t2 << t1 ------------------
    def __rshift__(self, other: OperatorArg) -> OperatorArg:
        return self._wire(other, downstream=True)

    def __lshift__(self, other: OperatorArg) -> OperatorArg:
        return self._wire(other, downstream=False)

    def _wire(self, other: OperatorArg, *, downstream: bool) -> OperatorArg:
        """``self >> other`` (downstream=True) либо ``self << other`` (downstream=False)."""
        items: list[BaseOperator] = (
            [*other] if isinstance(other, (list, tuple, set, frozenset)) else [other]  # type: ignore[list-item]
        )
        for op in items:
            self._assert_operator(op)
            if downstream:
                op.upstream_task_ids.add(self.task_id)
                self.downstream_task_ids.add(op.task_id)
            else:
                self.upstream_task_ids.add(op.task_id)
                op.downstream_task_ids.add(self.task_id)
        return other

    def _assert_operator(self, op: object) -> None:
        if not isinstance(op, BaseOperator):
            msg = (
                f"Операнд >>/<< должен быть оператором или их списком, получен {type(op).__name__}"
            )
            raise TypeError(msg)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{type(self).__name__} {self.task_id}>"


class DAG:
    """Shim ``airflow.sdk.DAG``: контекст-менеджер, собирающий задачи и рёбра."""

    _prev_dag: DAG | None = None  # для корректного восстановления при вложенных with

    def __init__(
        self,
        dag_id: str,
        *,
        default_args: dict[str, Any] | None = None,
        description: str | None = None,
        schedule: Any = None,
        start_date: Any = None,
        catchup: bool = True,
        tags: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.dag_id = dag_id
        _dag_registry[dag_id] = self
        self.default_args = default_args or {}
        self.description = description
        self.schedule = schedule
        self.start_date = start_date
        self.catchup = catchup
        self.tags = tags or []
        self.task_dict: dict[str, BaseOperator] = {}
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __enter__(self) -> DAG:
        global _current_dag
        self._prev_dag = _current_dag
        _current_dag = self
        return self

    def __exit__(self, *exc: object) -> None:
        global _current_dag
        _current_dag = self._prev_dag

    def add_task(self, task: BaseOperator) -> None:
        if task.task_id in self.task_dict:
            msg = f"Duplicate task_id {task.task_id!r} в DAG {self.dag_id!r}"
            raise ValueError(msg)
        self.task_dict[task.task_id] = task

    # -- интроспекция графа (то, что сравнивается в Части C) ---------------
    def edges(self) -> list[tuple[str, str]]:
        """Рёбра (upstream, downstream), отсортированные — для стабильного сравнения."""
        return sorted(
            (up, task.task_id) for task in self.task_dict.values() for up in task.upstream_task_ids
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"<DAG {self.dag_id}: {len(self.task_dict)} tasks>"
