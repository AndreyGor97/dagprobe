"""Shim: ``airflow.sdk`` — DAG, декораторы TaskFlow, Asset/Param (B3→B5 + Срез 30)."""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import DAG
from dagprobe.shim._taskflow import XComArg, task_group_factory
from dagprobe.shim._taskflow import dag as _dag_factory
from dagprobe.shim._taskflow import task as _task
from dagprobe.shim.airflow.models.param import Param
from dagprobe.shim.airflow.providers.standard.operators.python import get_current_context
from dagprobe.shim.airflow.sdk.definitions.asset import Asset

__all__ = [
    "Asset",
    "DAG",
    "Param",
    "XComArg",
    "dag",
    "get_current_context",
    "task",
    "task_group",
]

task = _task
task_group = task_group_factory
dag = _dag_factory  # DagFactory: и @dag(...), и @dag без скобок


def __getattr__(name: str) -> Any:
    msg = f"shim airflow.sdk: {name!r} is not implemented yet"
    raise AttributeError(msg)
