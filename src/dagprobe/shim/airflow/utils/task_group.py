"""Shim 2.x-пути: ``airflow.utils.task_group`` — TaskGroup (контекст-менеджер)."""

from dagprobe.shim._taskflow import TaskGroup

__all__ = ["TaskGroup"]
