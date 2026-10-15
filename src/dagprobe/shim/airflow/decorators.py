"""Shim 2.x-пути: ``airflow.decorators`` — TaskFlow-фабрики shim'а.

Вызов ``task_fn(...)`` внутри ``with DAG`` создаёт оператор (XComArg), тело
исполняется только в Task Test — как в реальном Airflow.
"""

from __future__ import annotations

from dagprobe.shim._taskflow import DagFactory, TaskFactory, task_group_factory

task = TaskFactory()
dag = DagFactory()
task_group = task_group_factory

__all__ = ["dag", "task", "task_group"]
