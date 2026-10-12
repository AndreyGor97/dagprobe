"""Срез 10: ShortCircuitOperator в прогоне, --fail-on у `list`, GitLab CI-шаблон.

Прогоны — в subprocess, по одному сценарию на класс поведения (как в Срезе 7).
"""

from __future__ import annotations

from pathlib import Path

from dagprobe import ci
from dagprobe.cli import main
from dagprobe.task_test import run_dag

GATE_TRUE = """
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG

with DAG("gate_true", schedule=None, start_date=None, catchup=False) as dag:

    def gate(**_):
        return True

    def work(**_):
        return "done"

    ShortCircuitOperator(task_id="gate", python_callable=gate) >> PythonOperator(
        task_id="work", python_callable=work
    )
"""

GATE_FALSE = GATE_TRUE.replace("gate_true", "gate_false").replace("return True", "return False")

GATE_NONE_CASCADE = """
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG

with DAG("gate_none_cascade", schedule=None, start_date=None, catchup=False) as dag:

    def gate(**_):
        return None  # falsy

    def leaf(**_):
        return 1

    gate_t = ShortCircuitOperator(task_id="gate", python_callable=gate)
    mid_t = PythonOperator(task_id="mid", python_callable=leaf)
    end_t = PythonOperator(task_id="end", python_callable=leaf)
    gate_t >> mid_t >> end_t
"""


def _write(tmp_path: Path, *sources: str) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    for i, src in enumerate(sources):
        (dags / f"dag{i}.py").write_text(src, encoding="utf-8")
    return dags


def test_gate_true_proceeds(tmp_path: Path) -> None:
    dags = _write(tmp_path, GATE_TRUE)
    run = run_dag(dags / "dag0.py", "gate_true")
    assert run.statuses == {"gate": "ok", "work": "ok"}
    assert run.values["gate"] in (True, "True")  # значения идут через JSON-roundtrip


def test_gate_false_skips_downstream(tmp_path: Path) -> None:
    dags = _write(tmp_path, GATE_FALSE)
    run = run_dag(dags / "dag0.py", "gate_false")
    # skip — не провал: gate сам ok, downstream skipped (как в Airflow)
    assert run.statuses == {"gate": "ok", "work": "skipped"}
    assert not run.failures


def test_gate_falsy_cascades_via_trigger_rule(tmp_path: Path) -> None:
    dags = _write(tmp_path, GATE_NONE_CASCADE)
    run = run_dag(dags / "dag0.py", "gate_none_cascade")
    # mid и end: скип транзитивный — ignore_downstream_trigger_rules=True (дефолт),
    # trigger_rule не участвуют (семантика Airflow 3.3.2, подтверждена A/B mode-b)
    assert run.statuses == {"gate": "ok", "mid": "skipped", "end": "skipped"}
    assert run.values["gate"] in (None, "None")  # JSON-roundtrip


def test_list_fail_on(tmp_path: Path, capsys) -> None:
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "bad.py").write_text("this is not python (", encoding="utf-8")
    assert main(["list", "--dags-folder", str(broken)]) == ci.FINDINGS
    assert main(["list", "--dags-folder", str(broken), "--fail-on", "never"]) == 0
