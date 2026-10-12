"""Срез 8: версионный профиль 2.10 + ``dagprobe audit 2to3``."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

from dagprobe.audit import audit_2to3
from dagprobe.cli import main
from dagprobe.shim import _templates

LD = datetime.fromisoformat("2026-09-29T00:00:00+00:00")

# эталон из G2 (demand_validation_report): легаси-ключи контекста, удалённые в 3.x
MIG_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def fail_on_empty(value):
    if not value:
        raise ValueError("легаси-ключ контекста не отрендерился")
    return value


def echo(value):
    return value


with DAG("mig_2to3"):
    PythonOperator(
        task_id="uses_execution_date",
        python_callable=fail_on_empty,
        op_kwargs={"value": "{{ execution_date }}"},
    )
    PythonOperator(
        task_id="uses_tomorrow_ds",
        python_callable=echo,
        op_kwargs={"value": "{{ tomorrow_ds }}"},
    )

with DAG("clean_2to3"):
    PythonOperator(task_id="plain", python_callable=echo, op_kwargs={"value": "{{ ds }}"})
"""


def test_profile_v2_adds_legacy_keys() -> None:
    task = SimpleNamespace(task_id="t", params={}, dag=SimpleNamespace(params={}))
    ctx = _templates.build_context(task, LD, "2.10")  # type: ignore[arg-type]
    assert ctx["execution_date"] == LD.replace(tzinfo=None)  # в 2.x — naive UTC
    assert ctx["tomorrow_ds"] == "2026-09-30"
    assert ctx["yesterday_ds"] == "2026-09-28"
    assert ctx["ti"].task_id == "t"  # ti в контексте с xcom_pull (Срез 9)


def test_profile_v3_has_no_legacy_keys() -> None:
    task = SimpleNamespace(task_id="t", params={}, dag=SimpleNamespace(params={}))
    ctx = _templates.build_context(task, LD)  # type: ignore[arg-type]
    assert "execution_date" not in ctx
    assert "tomorrow_ds" not in ctx
    with pytest.raises(ValueError, match="неизвестный профиль"):
        _templates.build_context(task, LD, "2.11")  # type: ignore[arg-type]


def test_audit_finds_legacy_context_divergence(tmp_path: object) -> None:
    (tmp_path / "mig.py").write_text(MIG_DAG, encoding="utf-8")
    findings = audit_2to3(tmp_path)  # type: ignore[arg-type]
    by_task = {(f["dag_id"], f["task_id"]): f for f in findings}

    # ok в 2.10, failed в 3.3 — падение рендера легаси-ключа
    st = by_task[("mig_2to3", "uses_execution_date")]
    assert st["code"] == "status" and st["in_v2"] == "ok" and st["in_v3"] == "failed"
    assert "легаси-ключ" in st["detail"]

    # ok в обоих, но отрендеренное значение разное
    val = by_task[("mig_2to3", "uses_tomorrow_ds")]
    assert val["code"] == "value" and val["in_v2"] == "'2026-09-30'" and val["in_v3"] == "''"

    # чистый DAG — ни одной находки
    assert not [f for f in findings if f["dag_id"] == "clean_2to3"]


def test_audit_single_dag_filter(tmp_path: object) -> None:
    (tmp_path / "mig.py").write_text(MIG_DAG, encoding="utf-8")
    findings = audit_2to3(tmp_path, dag_id="clean_2to3")  # type: ignore[arg-type]
    assert findings == []


def test_cli_audit_exit_codes(tmp_path: object, capsys: object) -> None:
    (tmp_path / "mig.py").write_text(MIG_DAG, encoding="utf-8")
    argv = ["audit", "2to3", "--dags-folder", str(tmp_path), "--format", "json"]

    assert main(argv) == 1  # находка по умолчанию валит exit
    assert json.loads(capsys.readouterr().out)["finding_count"] == 2

    assert main([*argv, "--fail-on", "never"]) == 0
    capsys.readouterr()

    # только чистый DAG — exit 0 и текст «находок нет»
    clean = tmp_path / "clean"
    clean.mkdir()
    (clean / "ok.py").write_text(
        "from airflow.providers.standard.operators.python import PythonOperator\n"
        "from airflow.sdk import DAG\n"
        "\n"
        "\n"
        "def echo(value):\n"
        "    return value\n"
        "\n"
        "\n"
        'with DAG("only_clean_2to3"):\n'
        "    PythonOperator(\n"
        '        task_id="plain", python_callable=echo, op_kwargs={"value": "{{ ds }}"}\n'
        "    )\n",
        encoding="utf-8",
    )
    code = main(["audit", "2to3", "--dags-folder", str(clean)])
    out = capsys.readouterr().out
    assert code == 0
    assert "no findings" in out
