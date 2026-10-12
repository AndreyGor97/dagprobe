"""Срез 8 (клин G4 из файла 18): поведенческий аудит миграции Airflow 2.10 → 3.x.

DAG прогоняется дважды на одних фикстурах — под профилем ``2.10`` (легаси-ключи
контекста рендера) и ``3.3`` — и результаты сравниваются. Каждое расхождение —
находка: поведение DAG меняется при миграции, статические правила (Ruff AIR)
такое не ловят, потому что это семантика вызова/рендера, а не синтаксис.

Находки (поля ``code``):
- ``status`` — статус задачи различается (типично: ok в 2.10, failed в 3.3);
- ``value`` — статусы одинаковые (ok), но XCom ``return_value`` различается
  (типично: легаси-ключ отрендерился в дату против пустой строки).

Покрытие — контекст рендера Jinja (G2 #4). XCom-семантика ``xcom_pull`` (G2
#1–#2) в прогоне ещё не эмулируется — находок этого класса не будет (Срез 9+).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dagprobe.analyze import TIMEOUT_S, analyze_file
from dagprobe.task_test import TaskRunResult, run_dag


def _diff_dag(dag_id: str, v2: TaskRunResult, v3: TaskRunResult) -> list[dict[str, Any]]:
    """Расхождения двух прогонов одного DAG: статусы, затем значения."""
    findings: list[dict[str, Any]] = []
    for tid in sorted(set(v2.statuses) | set(v3.statuses)):
        s2, s3 = v2.statuses.get(tid), v3.statuses.get(tid)
        if s2 != s3:
            failed = v3 if s3 == "failed" else v2
            tail = (failed.failures.get(tid) or "").strip().splitlines()[-1:]
            findings.append(
                {
                    "dag_id": dag_id,
                    "task_id": tid,
                    "code": "status",
                    "in_v2": s2,
                    "in_v3": s3,
                    "detail": tail[0] if tail else "",
                }
            )
        elif s2 == "ok" and v2.values.get(tid) != v3.values.get(tid):
            findings.append(
                {
                    "dag_id": dag_id,
                    "task_id": tid,
                    "code": "value",
                    "in_v2": v2.values.get(tid),
                    "in_v3": v3.values.get(tid),
                    "detail": "",
                }
            )
    return findings


def audit_2to3(
    dags_folder: Path | str,
    *,
    dag_id: str | None = None,
    fixtures: Path | str | None = None,
    timeout_s: float = TIMEOUT_S,
) -> list[dict[str, Any]]:
    """Аудит всех DAG каталога (или одного ``dag_id``): прогон 2.10 vs 3.3."""
    findings: list[dict[str, Any]] = []
    for f in sorted(Path(dags_folder).glob("*.py")):
        result = analyze_file(f, timeout_s=timeout_s)
        for did in result.dag_ids:
            if dag_id and did != dag_id:
                continue
            common: dict[str, Any] = {
                "fixtures_path": fixtures,
                "timeout_s": timeout_s,
            }
            v2 = run_dag(f, did, profile="2.10", **common)
            v3 = run_dag(f, did, profile="3.3", **common)
            findings.extend(_diff_dag(did, v2, v3))
    return findings
