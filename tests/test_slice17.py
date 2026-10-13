"""Срез 17: xcom_pull в шаблонах (долг Среза 7).

Срез 9 дал ``ti.xcom_pull`` в контексте (``build_context``), и этот же контекст
идёт в ``render_template_fields`` — шаблонная сторона уже работала, но не была
закреплена тестами. Здесь фиксируем: строковый/списковый ``task_ids`` и
отсутствующий XCom (→ ``None`` → ``'None'`` в строке) внутри ``op_args``.
A/B против живого Airflow 3.3.2 — DAG ``ab_xcom_template`` в ``spike/run_ab.py``.
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.task_test import run_dag

XCOM_TEMPLATE_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("xcom_template", schedule=None, start_date=None, catchup=False) as dag:

    def push(**_):
        return 42

    def check_str(v, **_):
        assert str(v) == "42", f"строковый task_ids: {v!r}"

    def check_list(v, **_):
        assert str(v) == "[42]", f"списковый task_ids из одного: {v!r}"

    def check_missing(v, **_):
        assert v == "None", f"отсутствующий XCom: {v!r}"

    p = PythonOperator(task_id="push", python_callable=push)
    s = PythonOperator(
        task_id="check_str", python_callable=check_str,
        op_args=["{{ ti.xcom_pull(task_ids='push') }}"],
    )
    l1 = PythonOperator(
        task_id="check_list", python_callable=check_list,
        op_args=["{{ ti.xcom_pull(task_ids=['push']) }}"],
    )
    m = PythonOperator(
        task_id="check_missing", python_callable=check_missing,
        op_args=["{{ ti.xcom_pull(task_ids='nope') }}"],
    )
    p >> [s, l1, m]
"""


def _run(tmp_path: Path, src: str, dag_id: str):
    file = tmp_path / f"{dag_id}.py"
    file.write_text(src, encoding="utf-8")
    return run_dag(file, dag_id)


def test_template_xcom_pull_str(tmp_path: Path) -> None:
    run = _run(tmp_path, XCOM_TEMPLATE_DAG, "xcom_template")
    assert run.statuses["check_str"] == "ok"


def test_template_xcom_pull_single_list(tmp_path: Path) -> None:
    run = _run(tmp_path, XCOM_TEMPLATE_DAG, "xcom_template")
    assert run.statuses["check_list"] == "ok"


def test_template_xcom_pull_missing_is_none(tmp_path: Path) -> None:
    run = _run(tmp_path, XCOM_TEMPLATE_DAG, "xcom_template")
    assert run.statuses["check_missing"] == "ok"


def test_template_xcom_pull_wrong_value_fails(tmp_path: Path) -> None:
    """Пушим не 42 — чекер обязан упасть: шаблон взял именно XCom, не литерал."""
    src = XCOM_TEMPLATE_DAG.replace("return 42", "return 7")
    run = _run(tmp_path, src, "xcom_template")
    assert run.statuses["check_str"] == "failed"
