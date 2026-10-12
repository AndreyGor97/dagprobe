"""CI-режим (§6.21): exit-коды, gate-уровни, JUnit XML, «только изменённые файлы».

Единственное место, где находки превращаются в код возврата:

    0 — находок уровня gate нет
    1 — есть находки уровня gate (по умолчанию error; ``--fail-on warning`` расширяет)
    2 — прогон не состоялся (DAG не найден, git недоступен, ошибка аргументов)

JUnit XML собирается stdlib-``xml.etree`` — экранирование сообщений не ручное.
"""

from __future__ import annotations

import subprocess
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

from dagprobe.errors import ERROR, TASK_FAILURE, UPSTREAM_FAILED, WARNING, DAGProbeError

OK = 0
FINDINGS = 1
USAGE = 2

_GATE: dict[str, tuple[str, ...]] = {
    "error": (ERROR,),
    "warning": (ERROR, WARNING),
    "never": (),
}


class CiError(RuntimeError):
    """Прогон не состоялся (нет git / нет базовой ветки) — это exit 2, а не находка."""


def exit_code(errors: Iterable[DAGProbeError], *, fail_on: str = "error") -> int:
    """Код возврата по gate-уровню (§6.21): warning по умолчанию CI не роняет."""
    levels = _GATE[fail_on]
    return FINDINGS if any(e.severity in levels for e in errors) else OK


def run_findings(failures: dict[str, str]) -> list[DAGProbeError]:
    """Провалы прогона — находки той же модели (§11.9), чтобы формат вывода был один."""
    return [
        DAGProbeError(code=TASK_FAILURE, severity=ERROR, message=msg, file=task_id)
        for task_id, msg in failures.items()
    ]


# (имя теста для CI, находки, исход: ok/failed/skipped)
Case = tuple[str, list[DAGProbeError], str]


def lint_cases(errors: Iterable[DAGProbeError], files: Iterable[Path]) -> list[Case]:
    """По одному тесту на файл DAG: находки файла → failed, чистый файл → ok."""
    by_file: dict[str, list[DAGProbeError]] = {f.name: [] for f in files}
    for e in errors:
        by_file.setdefault(e.file or "<unknown>", []).append(e)
    return [(name, errs, "failed" if errs else "ok") for name, errs in sorted(by_file.items())]


def run_cases(dag_id: str, statuses: dict[str, str], failures: dict[str, str]) -> list[Case]:
    """По одному тесту на задачу прогона: failed с трейсбеком, upstream_failed, skipped, ok."""
    cases: list[Case] = []
    for task_id, status in statuses.items():
        if task_id in failures:
            errs = run_findings({task_id: failures[task_id]})
        elif status == "upstream_failed":
            errs = [
                DAGProbeError(
                    code=UPSTREAM_FAILED,
                    severity=ERROR,
                    message="триггер-правило не выполнено: upstream пропущен или упал",
                    file=task_id,
                )
            ]
        else:
            errs = []
        cases.append((f"{dag_id}::{task_id}", errs, "failed" if errs else status))
    return cases


def findings_of(cases: Iterable[Case]) -> list[DAGProbeError]:
    return [e for _name, errs, _outcome in cases for e in errs]


def _location(name: str, e: DAGProbeError) -> str:
    """`файл:строка` (строка — если известна); для прогона вместо файла — имя задачи."""
    where = e.file or name
    return f"{where}:{e.line}" if e.line else where


def to_text(cases: Iterable[Case]) -> str:
    """Человекочитаемо: строка на находку, `name: ok` — на чистый тест."""
    lines = [f"{name}: {outcome}" for name, _errs, outcome in cases]
    lines += [
        f"  {_location(name, e)}: {e.severity} {e.code}: {e.message}"
        for name, errs, _outcome in cases
        for e in errs
    ]
    return "\n".join(lines) + "\n"


def to_junit(cases: Iterable[Case], *, time_s: float = 0.0) -> str:
    """JUnit XML: тест на файл/задачу, находки — в `<failure>` (§6.21, DoD п.5)."""
    items = list(cases)
    failed = sum(1 for _n, _e, outcome in items if outcome == "failed")
    root = ET.Element("testsuites")
    suite = ET.SubElement(
        root,
        "testsuite",
        name="dagprobe",
        tests=str(len(items)),
        failures=str(failed),
        skipped=str(sum(1 for _n, _e, outcome in items if outcome == "skipped")),
        time=f"{time_s:.3f}",
    )
    for name, errs, outcome in items:
        case = ET.SubElement(suite, "testcase", classname="dagprobe", name=name)
        if outcome == "skipped":
            ET.SubElement(case, "skipped")
        elif errs:
            head = errs[0]
            failure = ET.SubElement(
                case, "failure", type=head.code, message=f"{head.code}: {head.message}"
            )
            failure.text = "\n".join(f"{_location(name, e)}: {e.message}" for e in errs)
    return '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding="unicode")


def changed_python_files(dags_folder: Path, base_ref: str) -> list[Path]:
    """`*.py` из diff к базовой ветке (§6.21) — режим «только изменённые файлы»."""
    if not dags_folder.is_dir():
        raise CiError(f"каталог DAG не найден: {dags_folder}")
    root = Path(_git("rev-parse", "--show-toplevel", cwd=dags_folder).strip())
    folder = dags_folder.resolve()
    out: list[Path] = []
    diff = _git("diff", "--name-only", "--diff-filter=ACMR", f"{base_ref}...HEAD", cwd=root)
    for line in diff.splitlines():
        path = (root / line.strip()).resolve()
        if path.suffix == ".py" and folder in path.parents and path.exists():
            out.append(path)
    return out


def _git(*args: str, cwd: Path | None = None) -> str:
    """Тонкая обёртка над git: нет репозитория/ветки — громкий CiError (exit 2)."""
    try:
        proc = subprocess.run(("git", *args), cwd=cwd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:  # git не установлен
        raise CiError("git не найден — режим --changed-only недоступен") from exc
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip()
        raise CiError(f"git {' '.join(args)}: {detail}")
    return proc.stdout


def with_location(errors: Iterable[DAGProbeError], file: str) -> list[DAGProbeError]:
    """Дописать имя файла находкам, пришедшим без него (анализ отдаёт имя сам)."""
    return [e if e.file else replace(e, file=file) for e in errors]
