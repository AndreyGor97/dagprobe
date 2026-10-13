"""Shim: ``airflow.sdk`` — DAG и декораторы TaskFlow (покрывается инкрементально, B3→B5)."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from dagprobe.shim._core import DAG
from dagprobe.shim._taskflow import XComArg, task_group_factory
from dagprobe.shim._taskflow import task as _task

__all__ = ["DAG", "XComArg", "dag", "task", "task_group"]

task = _task
task_group = task_group_factory


def dag(**dag_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """``@dag(schedule=..., ...)`` — декоратор фабрики DAG (dag_id = имя функции)."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def factory(*args: Any, **kwargs: Any) -> DAG:
            with DAG(dag_kwargs.pop("dag_id", fn.__name__), **dag_kwargs) as d:
                fn(*args, **kwargs)
            return d

        return factory

    return decorator


def __getattr__(name: str) -> Any:
    msg = f"shim airflow.sdk: {name!r} is not implemented yet"
    raise AttributeError(msg)
