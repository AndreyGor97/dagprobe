"""Срез 21: режим Б (живой Airflow) как продукт — ``dagprobe ab``."""

from __future__ import annotations

import pytest

from dagprobe.cli import main
from dagprobe.live import _constraints_url, compare_statuses


def test_compare_identical_statuses() -> None:
    a = {"dag": {"t1": "ok", "t2": "skipped"}}
    b = {"dag": {"t1": "success", "t2": "skipped"}}
    assert compare_statuses(a, b) == []


def test_compare_maps_shim_statuses_to_airflow() -> None:
    a = {"dag": {"t": "upstream_failed"}}
    b = {"dag": {"t": "success"}}
    assert compare_statuses(a, b) == [
        {"dag_id": "dag", "task_id": "t", "in_a": "upstream_failed", "in_b": "success"}
    ]


def test_compare_missing_task_or_dag_is_a_finding() -> None:
    # задача есть только в шиме (живой Airflow её не импортировал) — находка, не тишина
    findings = compare_statuses({"dag": {"only_a": "ok"}}, {"dag": {}})
    assert findings == [{"dag_id": "dag", "task_id": "only_a", "in_a": "success", "in_b": None}]
    assert compare_statuses({}, {"dag": {"only_b": "failed"}}) == [
        {"dag_id": "dag", "task_id": "only_b", "in_a": None, "in_b": "failed"}
    ]


def test_constraints_url() -> None:
    assert _constraints_url("3.3.2", "3.14") == (
        "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt"
    )


def test_cli_ab_exit_codes(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "dagprobe.cli.ab_command", lambda *a, **k: ({"finding_count": 0, "findings": []}, 0)
    )
    assert main(["ab", "--dags-folder", "dags"]) == 0
    assert "no findings" in capsys.readouterr().out

    monkeypatch.setattr(
        "dagprobe.cli.ab_command",
        lambda *a, **k: (
            {
                "finding_count": 1,
                "findings": [{"dag_id": "d", "task_id": "t", "in_a": "success", "in_b": "failed"}],
            },
            1,
        ),
    )
    assert main(["ab", "--dags-folder", "dags"]) == 1
    assert "DAG d / t: success (shim) → failed (live)" in capsys.readouterr().out

    # прогон не состоялся — exit 2 с JSON-ошибкой (как в audit/run)
    monkeypatch.setattr(
        "dagprobe.cli.ab_command", lambda *a, **k: ({"error": "no DAGs in dags"}, 2)
    )
    assert main(["ab", "--dags-folder", "dags"]) == 2
    assert "no DAGs" in capsys.readouterr().out
