"""Срез 6 (§6.21): CI-режим — форматы, exit-коды, gate-уровни, «изменённые файлы».

Проверяется то, на что опирается CI: код возврата, JUnit XML, который парсит
stdlib-парсер (=любой потребитель отчёта), и отбор файлов из diff к базовой ветке.
"""

from __future__ import annotations

import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from dagprobe import ci
from dagprobe.cli import main
from dagprobe.errors import ERROR, WARNING, DAGProbeError

CLEAN_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("clean", schedule=None, start_date=None, catchup=False) as dag:
    PythonOperator(task_id="noop", python_callable=lambda **_: None)
"""

WARN_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("warny", schedule=None, start_date=None) as dag:
    PythonOperator(task_id="noop", python_callable=lambda **_: None)
"""

FAIL_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("boom", schedule=None, start_date=None, catchup=False) as dag:
    def bad(**_):
        raise ValueError("нарочно")

    PythonOperator(task_id="bad", python_callable=bad)
"""

BROKEN_DAG = "from airflow.sdk import DAG\n\nwith DAG(\n"  # незакрытая скобка


def _dags(tmp_path: Path, **sources: str) -> Path:
    folder = tmp_path / "dags"
    folder.mkdir(parents=True, exist_ok=True)
    for name, source in sources.items():
        (folder / f"{name}.py").write_text(source, encoding="utf-8")
    return folder


def _git(repo: Path, *args: str) -> None:
    subprocess.run(("git", *args), cwd=repo, check=True, capture_output=True)


def test_lint_clean_folder_exits_zero_and_prints_ok(tmp_path: Path, capsys) -> None:
    """Чистый каталог: exit 0, в text-выводе — непустой `ok` по файлу (§6.21)."""
    code = main(["lint", "--dags-folder", str(_dags(tmp_path, clean=CLEAN_DAG))])
    out = capsys.readouterr().out
    assert code == ci.OK
    assert "clean.py: ok" in out


def test_lint_broken_dag_exits_one_in_all_formats(tmp_path: Path, capsys) -> None:
    """Сломанный DAG: exit 1 в любом формате — CI обязан это увидеть."""
    folder = _dags(tmp_path, broken=BROKEN_DAG)
    assert main(["lint", "--dags-folder", str(folder), "--format", "json"]) == ci.FINDINGS
    assert "IMPORT_FAILURE" in capsys.readouterr().out

    assert main(["lint", "--dags-folder", str(folder), "--format", "junit"]) == ci.FINDINGS
    root = ET.fromstring(capsys.readouterr().out)  # валидный XML = парсится потребителем
    assert root.find("testsuite").attrib["failures"] == "1"  # type: ignore[union-attr]
    assert root.find(".//failure").attrib["type"] == "IMPORT_FAILURE"  # type: ignore[union-attr]


def test_gate_levels_choose_what_breaks_ci(tmp_path: Path) -> None:
    """Gate-уровни (§6.21): warning роняет только при `--fail-on warning`."""
    folder = _dags(tmp_path, warny=WARN_DAG)
    assert main(["lint", "--dags-folder", str(folder)]) == ci.OK
    assert main(["lint", "--dags-folder", str(folder), "--fail-on", "warning"]) == ci.FINDINGS
    assert main(["lint", "--dags-folder", str(folder), "--fail-on", "never"]) == ci.OK
    error_folder = _dags(tmp_path / "err", broken=BROKEN_DAG)
    assert main(["lint", "--dags-folder", str(error_folder), "--fail-on", "never"]) == ci.OK


def test_junit_escapes_messages() -> None:
    """Экранирование — на stdlib-xml: `<`/`&`/кавычки в сообщении не ломают отчёт."""
    finding = DAGProbeError(code="RULE", severity=WARNING, message='a < b & "c"', line=3)
    xml = ci.to_junit([("x.py", [finding], "failed")])
    failure = ET.fromstring(xml).find(".//failure")
    assert failure is not None and 'a < b & "c"' in (failure.text or "")


def test_changed_only_takes_files_from_diff(tmp_path: Path, capsys) -> None:
    """`--changed-only`: линтуются только файлы из diff к базовой ветке (§6.21)."""
    repo = tmp_path / "repo"
    folder = _dags(repo, untouched=CLEAN_DAG, touched=CLEAN_DAG)
    (repo / "README.md").write_text("не DAG\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "-c", "user.email=t@e", "-c", "user.name=t", "add", "-A")
    _git(repo, "-c", "user.email=t@e", "-c", "user.name=t", "commit", "-qm", "base")
    (folder / "touched.py").write_text(WARN_DAG.replace('"warny"', '"touched"'), encoding="utf-8")
    _git(repo, "-c", "user.email=t@e", "-c", "user.name=t", "commit", "-qam", "warn")

    only = ci.changed_python_files(folder, "HEAD~1")
    assert [p.name for p in only] == ["touched.py"]
    assert (
        main(
            [
                "lint",
                "--dags-folder",
                str(folder),
                "--changed-only",
                "--base-ref",
                "HEAD~1",
                "--fail-on",
                "warning",
            ]
        )
        == ci.FINDINGS
    )
    assert capsys.readouterr().out.startswith("touched.py:")


def test_changed_only_without_git_is_loud(tmp_path: Path, monkeypatch, capsys) -> None:
    """Вне git-репозитория режим не молчит: exit 2 и причина в JSON."""
    folder = _dags(tmp_path, clean=CLEAN_DAG)
    monkeypatch.chdir(tmp_path)
    code = main(["lint", "--dags-folder", str(folder), "--changed-only"])
    assert code == ci.USAGE
    assert "error" in capsys.readouterr().out


def test_run_junit_reports_failed_task_and_gate(tmp_path: Path, capsys) -> None:
    """Прогон: провал задачи — `<failure>` с трейсбеком, exit 1; `--fail-on never` — 0."""
    folder = _dags(tmp_path, boom=FAIL_DAG)
    argv = ["run", "--dags-folder", str(folder), "--dag-id", "boom", "--format", "junit"]
    assert main(argv) == ci.FINDINGS
    root = ET.fromstring(capsys.readouterr().out)
    assert root.find("testsuite").attrib["failures"] == "1"  # type: ignore[union-attr]
    failure = root.find(".//failure")
    assert failure is not None and "нарочно" in (failure.text or "")
    assert main([*argv, "--fail-on", "never"]) == ci.OK


def test_run_text_format_lists_task_statuses(tmp_path: Path, capsys) -> None:
    """`run --format text` — строки по задачам, а не JSON (для логов CI)."""
    folder = _dags(tmp_path, boom=FAIL_DAG)
    argv = ["run", "--dags-folder", str(folder), "--dag-id", "boom", "--format", "text"]
    assert main(argv) == ci.FINDINGS
    assert capsys.readouterr().out.startswith("boom::bad: failed")


def test_missing_dag_is_usage_error_not_finding(tmp_path: Path, capsys) -> None:
    """«DAG не найден» — exit 2 (прогон не состоялся), а не находка для gate."""
    folder = _dags(tmp_path, clean=CLEAN_DAG)
    code = main(["run", "--dags-folder", str(folder), "--dag-id", "нет_такого"])
    assert code == ci.USAGE
    assert "error" in capsys.readouterr().out


@pytest.mark.parametrize("gate", ["error", "warning", "never"])
def test_exit_code_matches_gate_table(gate: str) -> None:
    """Таблица exit-кодов §6.21 зафиксирована отдельно от CLI."""
    warning = DAGProbeError(code="X", severity=WARNING, message="m")
    error = DAGProbeError(code="Y", severity=ERROR, message="m")
    expected = {"error": ci.OK, "warning": ci.FINDINGS, "never": ci.OK}
    assert ci.exit_code([warning], fail_on=gate) == expected[gate]
    assert ci.exit_code([error], fail_on=gate) == (ci.FINDINGS if gate != "never" else ci.OK)
