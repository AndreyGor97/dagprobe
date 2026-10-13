"""Срез 9: xcom_pull-семантика в прогоне (G2 #1–#2) + get_current_context."""

from __future__ import annotations

from pathlib import Path

import pytest

from dagprobe.audit import audit_2to3
from dagprobe.shim._templates import get_current_context

ETALON = Path("examples/dags/migration_xcom.py")


def _findings() -> dict[tuple[str, str], dict]:
    fnd = audit_2to3(ETALON.parent, dag_id="migration_xcom")
    return {(f["task_id"], f["code"]): f for f in fnd}


def test_xcom_none_semantics() -> None:
    """G2 #1: task_ids=None — 2.x весь dag-run, 3.x только текущая задача → None."""
    st = _findings()[("taker_none", "status")]
    assert st["in_v2"] == "ok" and st["in_v3"] == "failed"
    assert "вернул None" in st["detail"]


def test_xcom_single_list_semantics() -> None:
    """G2 #2 (#49540) опровергнут живым A/B (13.10, 3.3.2): список → список в обеих версиях."""
    fnd = audit_2to3(ETALON.parent, dag_id="migration_xcom")
    assert not [f for f in fnd if f["task_id"] == "taker_list"]


def test_xcom_string_task_ids_no_finding() -> None:
    """Контроль: строковый task_ids одинаков в обеих версиях."""
    assert not [
        f for f in audit_2to3(ETALON.parent, dag_id="migration_xcom") if f["task_id"] == "taker_str"
    ]


def test_get_current_context_loud_outside_run() -> None:
    with pytest.raises(RuntimeError, match="inside task execution"):
        get_current_context()
