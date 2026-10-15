"""Shim 2.x-пути ``airflow.models`` — классы метастора как стабы (parse-time).

DagRun/TaskInstance/XCom нужны на этапе parse только как имена (type-хинты,
запросы внутри функций); реальный рантайм метастора — движок Airflow.
"""

from __future__ import annotations

from dagprobe.shim._core import DAG, BaseOperator, Variable
from dagprobe.shim.airflow.models.dagbag import DagBag
from dagprobe.shim.airflow.models.param import DagParam, Param, ParamsDict
from dagprobe.shim.airflow.sdk.bases.hook import Connection

__all__ = [
    "BaseOperator",
    "Connection",
    "DAG",
    "DagBag",
    "DagModel",
    "DagParam",
    "DagRun",
    "Param",
    "ParamsDict",
    "TaskFail",
    "TaskInstance",
    "Variable",
    "XCom",
]


class DagRun:  # noqa: D101 — стаб
    pass


class TaskInstance:  # noqa: D101 — стаб
    pass


class DagModel:  # noqa: D101 — стаб
    pass


class TaskFail:  # noqa: D101 — стаб
    pass


class XCom:  # noqa: D101 — стаб (значения XCom — в shim.xcom)
    pass
