"""Срез 1 (файл 18, F2–F5): analyze_file + CLI list на эталонных DAG спайка.

Дымовой тест CI: все эталонные DAG собираются шимом без ошибок, структура
совпадает с зафиксированной сверкой A/B (spike_report: 7/7 идентичны).
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.analyze import analyze_file
from dagprobe.cli import list_command

SPIKE_DAGS = Path(__file__).parent.parent / "spike" / "dags"

# ожидаемое число задач по DAG — из таблицы сверки A/B (Docs/spike_report.md)
EXPECTED_TASKS = {
    "example_branch_dop_operator_v3": 3,
    "example_dynamic_task_mapping": 2,
    "example_task_group_mapping": 2,
    "example_task_mapping_second_order": 3,
    "example_workday_timetable": 1,
    "tutorial": 3,
    "tutorial_taskflow_api": 3,
}


def test_analyze_file_tutorial() -> None:
    result = analyze_file(SPIKE_DAGS / "tutorial.py")
    assert result.errors == []
    assert result.dag_ids == ["tutorial"]
    dag = result.dags[0]
    assert set(dag.tasks) == {"print_date", "sleep", "templated"}
    assert [tuple(e) for e in dag.edges] == [("print_date", "sleep"), ("print_date", "templated")]


def test_analyze_file_error_is_result_not_crash() -> None:
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write("def broken(:\n")
        path = f.name
    result = analyze_file(path)
    assert result.dag_ids == []
    assert len(result.errors) == 1
    assert "SyntaxError" in result.errors[0].message
    assert result.errors[0].line == 1  # локация в пользовательском коде


def test_cli_list_spike_dags() -> None:
    result = list_command(SPIKE_DAGS)
    # только warnings линтера на эталонных DAG (например implicit catchup),
    # import-ошибок быть не должно
    assert [e for e in result.errors if e.severity == "error"] == []
    assert result.dag_ids == sorted(EXPECTED_TASKS)
    assert {d.dag_id: len(d.tasks) for d in result.dags} == EXPECTED_TASKS


def test_lint_catches_real_issues() -> None:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        dag = Path(td) / "bad.py"
        dag.write_text(
            "from airflow.sdk import DAG\n"
            'DAG("d", schedule=None, start_date=dt(2020, 1, 1),\n'
            "    catchup=True,\n"  # явный catchup — не флагается
            ")\n"
            "x = datetime.now()\n"  # NONDETERMINISTIC_TIME
            'DAG("d2", schedule_interval=None)\n',  # DEPRECATED_API + implicit catchup
            encoding="utf-8",
        )
        from dagprobe.lint import lint_file

        found = lint_file(dag)
        codes = [e.code for e in found]
        assert "NONDETERMINISTIC_TIME" in codes
        assert "DEPRECATED_API" in codes
        assert codes.count("IMPLICIT_CATCHUP") == 1  # только d2


def test_lint_inline_ignore() -> None:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        dag = Path(td) / "ignored.py"
        dag.write_text(
            "x = datetime.now()  # dagprobe: ignore=NONDETERMINISTIC_TIME\n",
            encoding="utf-8",
        )
        from dagprobe.lint import lint_file

        assert lint_file(dag) == []
