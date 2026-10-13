"""Механика TaskFlow API для shim'а: ``@dag``, ``@task``, ``@task.branch``, ``task_group``.

Спайк-минимум (файл 18, B5): построение графа, зависимости через передачу результатов
задач в аргументы, Dynamic Task Mapping (``.expand()``). Исполнение функций задач
не производится — это уровень Части C/D, не построения графа.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterable
from typing import Any

from dagprobe.shim import _core


class XComArg:
    """Результат задачи как значение: ``load(order_summary["key"])``, ``sum_it(added)``."""

    def __init__(self, task: _core.BaseOperator, key: str | None = None) -> None:
        self.task = task
        self.key = key

    def __getitem__(self, key: str) -> XComArg:
        return XComArg(self.task, key)  # multiple_outputs: доступ к ключу словаря

    # В Airflow XComArg сам поддерживает >>/<< — проксируем на задачу.
    def __rshift__(self, other: Any) -> Any:
        return self.task >> other

    def __lshift__(self, other: Any) -> Any:
        return self.task << other


class TaskFactory:
    """``@task`` / ``@task(**opts)`` / ``@task.branch`` — фабрика задач TaskFlow."""

    def __init__(self, *, branch: bool = False) -> None:
        self._branch = branch

    def __call__(self, fn: Callable[..., Any] | None = None, **opts: Any) -> Any:
        if fn is None:  # вызван с опциями: @task(multiple_outputs=True)
            return lambda f: self._wrap(f, opts)
        return self._wrap(fn, opts)

    def _wrap(self, fn: Callable[..., Any], opts: dict[str, Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def call(*args: Any, **kwargs: Any) -> XComArg:
            dag = _core.current_dag()
            if dag is None:
                msg = f"@task {fn.__name__!r} called outside a DAG context"
                raise RuntimeError(msg)
            task_id = opts.get("task_id", fn.__name__)
            # Регистрацию делает сам BaseOperator.__init__ (через _current_dag) — не дублируем.
            task = _PythonTask(task_id=task_id, python_callable=fn, is_branch=self._branch)
            # Аргументы вызова сохраняем для рантайма (Task Test резолвит XComArg → XCom)
            task.op_args, task.op_kwargs = args, kwargs
            # Аргументы-результаты других задач → рёбра зависимостей
            for value in (*args, *kwargs.values()):
                _wire_arg(task, value)
            return XComArg(task)

        call.expand = _Expand(call)  # type: ignore[attr-defined]
        return call

    @property
    def branch(self) -> TaskFactory:
        return TaskFactory(branch=True)


class _Expand:
    """``add_one.expand(x=[1, 2, 3])`` — Dynamic Task Mapping."""

    def __init__(self, call: Callable[..., XComArg]) -> None:
        self._call = call

    def __call__(self, **kwargs: Any) -> XComArg:
        arg = next(iter(kwargs.values()))  # спайк: один expand-аргумент
        handle = self._call(**{f"__mapped_{k}": v for k, v in kwargs.items()})
        _mark_mapped(handle)
        _wire_arg(handle.task, arg)
        return handle


def _mark_mapped(handle: XComArg) -> None:
    task = handle.task
    assert isinstance(task, _PythonTask)
    task.mapped = True


def _wire_arg(task: _core.BaseOperator, value: Any) -> None:
    if isinstance(value, XComArg):
        value.task >> task
    elif isinstance(value, Iterable) and not isinstance(value, str | bytes | dict):
        for item in value:
            _wire_arg(task, item)


class _PythonTask(_core.BaseOperator):
    def __init__(
        self, task_id: str, python_callable: Callable[..., Any], *, is_branch: bool = False
    ) -> None:
        super().__init__(task_id=task_id)
        self.python_callable = python_callable
        self.is_branch = is_branch
        self.mapped = False
        self.op_args: tuple[Any, ...] = ()  # аргументы вызова — для рантайма (Task Test)
        self.op_kwargs: dict[str, Any] = {}


class TaskGroup:
    """``@task_group``: задачи внутри получают префикс ``group.`` (id как в Airflow)."""

    def __init__(self, group_id: str) -> None:
        self.group_id = group_id

    def __enter__(self) -> TaskGroup:
        self._prev = _core.current_group()
        _core.set_current_group(self)
        return self

    def __exit__(self, *exc: object) -> None:
        _core.set_current_group(self._prev)


def task_group_factory(fn: Callable[..., Any]) -> Callable[..., Any]:
    """``@task_group`` на функции: вызов создаёт группу и исполняет тело в её контексте.

    ponytail: спайк регистрирует задачи без префикса ``group.`` у task_id — структура
    Airflow (``group.add_1``) вернётся, когда сравнение Mode B (Часть C) её потребует.
    """

    @functools.wraps(fn)
    def call(*args: Any, **kwargs: Any) -> Any:
        dag = _core.current_dag()
        if dag is None:
            msg = f"@task_group {fn.__name__!r} called outside a DAG context"
            raise RuntimeError(msg)
        with TaskGroup(fn.__name__):
            result = fn(*args, **kwargs)
        return result

    call.expand = _GroupExpand(call)  # type: ignore[attr-defined]
    return call


class _GroupExpand:
    """``op.expand(num=[1, 2, 3])``: на парсинге тело группы исполняется один раз,
    задачи помечаются mapped; раскрытие по значениям — рантайм (Часть C, не спайк)."""

    def __init__(self, call: Callable[..., Any]) -> None:
        self._call = call

    def __call__(self, **kwargs: Any) -> None:
        values = next(iter(kwargs.values()))
        first = values[0] if isinstance(values, list) and values else None
        self._call(first)


# ``from airflow.sdk import task`` — один общий экземпляр фабрики; ``task.branch`` — свой.
task = TaskFactory()
