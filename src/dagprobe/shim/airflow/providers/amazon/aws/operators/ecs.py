"""Shim ``providers.amazon.aws.operators.ecs`` (корпус RealDag2, Срез 30)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class EcsCreateClusterOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EcsDeleteClusterOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EcsDeregisterTaskDefinitionOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EcsRegisterTaskDefinitionOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


class EcsRunTaskOperator(BaseOperator):
    """Стаб: аргументы — атрибуты; исполнение — рантайм Airflow/AWS."""


__all__ = [
    "EcsCreateClusterOperator",
    "EcsDeleteClusterOperator",
    "EcsDeregisterTaskDefinitionOperator",
    "EcsRegisterTaskDefinitionOperator",
    "EcsRunTaskOperator",
]
