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


class Variable:
    """Стаб ``Variable`` метастора: parse-time — хранение, ``get`` — fixtures/дефолт.

    ``Variable.get(key, default_var=...)`` возвращает дефолт (как в Airflow при
    отсутствии ключа); без дефолта — громкая ошибка: значение берёт движок Airflow.
    """

    _NO_DEFAULT = object()

    def __init__(self, key: str | None = None, val: Any = None, **kwargs: Any) -> None:
        self.key = key
        self.val = val

    @classmethod
    def get(cls, key: str, default_var: Any = _NO_DEFAULT, deserialize_json: bool = False) -> Any:
        if default_var is not cls._NO_DEFAULT:
            return default_var
        msg = (
            f"Variable {key!r}: значения нет ни в fixtures, ни в default_var — "
            "метастор Airflow в шиме не эмулируется"
        )
        raise RuntimeError(msg)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Variable {self.key}>"


class Dataset:
    """Стаб ``airflow.Dataset`` (в 3.x — алиас Asset): uri + extra, граф не трогает."""

    template_fields = ("uri",)

    def __init__(self, uri: str, *, extra: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self.uri = uri
        self.extra = extra or {}


class BaseOperator:
    """Общий базовый класс операторов: регистрация в DAG + зависимости."""

    task_id: str
    dag: DAG
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]
    template_fields: tuple[str, ...] = ()
    mapped: bool  # Dynamic Task Mapping (только через .partial().expand() / TaskFlow)

    def __init__(self, task_id: str, dag: DAG | None = None, **kwargs: Any) -> None:
        group = current_group()
        if group is not None:
            task_id = f"{group.group_id}.{task_id}"  # как в Airflow: «op.add_1»
            group.members.append(self)
        self.task_id = task_id
        self.mapped = False
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        self.params: dict[str, Any] = {}
        for key, value in kwargs.items():
            setattr(self, key, value)  # default_args и явные аргументы — как атрибуты
        target = dag if dag is not None else _current_dag
        if target is None:
            msg = f"Operator {task_id!r} created outside a DAG context"
            raise RuntimeError(msg)
        self.dag = target
        target.add_task(self)

    @property
    def output(self) -> TaskFlow_XComArg:
        """``task.output`` — результат задачи как XComArg (как в Airflow)."""
        from dagprobe.shim._taskflow import XComArg

        return XComArg(self)

    @classmethod
    def partial(cls, **kwargs: Any) -> Any:
        """``PythonOperator.partial(...)`` — static-kwargs + ``.expand()`` (как в Airflow)."""
        from dagprobe.shim._taskflow import OperatorPartial

        return OperatorPartial(cls, kwargs)

    # -- зависимости: t1 >> t2 / t1 >> [t2, t3] / t2 << t1 ------------------
    def __rshift__(self, other: OperatorArg) -> OperatorArg:
        return self._wire(other, downstream=True)

    def __lshift__(self, other: OperatorArg) -> OperatorArg:
        return self._wire(other, downstream=False)

    # ``[t1, t2] >> t3``: слева список без __rshift__ — Python зовёт __rrshift__ справа;
    # self здесь НИЖНИЙ по потоку (other — верхний), возвращаем self для цепочек
    def __rrshift__(self, other: OperatorArg) -> OperatorArg:
        self._wire(other, downstream=False)
        return self

    def __rlshift__(self, other: OperatorArg) -> OperatorArg:
        self._wire(other, downstream=True)
        return self

    # явный API Airflow (корпус RealDag2: ``t2.set_upstream(t1)``)
    def set_upstream(self, other: OperatorArg) -> None:
        self._wire(other, downstream=False)

    def set_downstream(self, other: OperatorArg) -> None:
        self._wire(other, downstream=True)

    def _wire(self, other: OperatorArg, *, downstream: bool) -> OperatorArg:
        """``self >> other`` (downstream=True) либо ``self << other`` (False).

        Операндом может быть XComArg (``op >> task_fn(...)`` в TaskFlow) —
        как в Airflow, проводка идёт на лежащую в нём задачу.
        """
        from dagprobe.shim._taskflow import TaskGroup, XComArg

        items_raw: list[Any] = (
            [*other] if isinstance(other, (list, tuple, set, frozenset)) else [other]
        )
        items = [item.task if isinstance(item, XComArg) else item for item in items_raw]
        for op in items:
            if isinstance(op, TaskGroup):
                # op >> group: в корни группы; op << group: из листьев группы
                op._wire_node(self, node_is_downstream=not downstream)
                continue
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
                f">>/< operand must be an operator or a list of operators, got {type(op).__name__}"
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
            msg = f"Duplicate task_id {task.task_id!r} in DAG {self.dag_id!r}"
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
