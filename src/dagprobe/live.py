"""Режим Б как продукт (Срез 21): живой A/B — шим против честного Airflow.

Одна команда вместо рассыпанных по CI/`.venv-mode-b`/``spike/run_ab.py`` шагов:
создать venv с ``apache-airflow==<version>`` (constraints) через ``uv``, поднять
SQLite-метабазу, синкнуть DAGS_FOLDER, прогнать каждый DAG дважды — шимом
(``run_dag``, профиль 3.3) и живым ``DAG.test()`` в venv — и сравнить статусы.

Каждое расхождение — находка (как в ``audit``): шим врёт против референса.
Fixtures применяются только к режиму A (шим): живой Airflow исполняет задачи
по-настоящему, стабов у него нет. Exit-коды как в ``audit``: 0 — совпадает,
1 — есть находки, 2 — прогон не состоялся.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

from dagprobe.analyze import LOGICAL_DATE, analyze_file
from dagprobe.task_test import run_dag

LIVE_TIMEOUT_S = 600.0  # живой DAG.test() медленнее шима: интерпретатор + БД на каждый прогон

# статус шима → состояние Airflow (cf. spike/run_ab.py._TO_B)
_TO_AIRFLOW = {
    "ok": "success",
    "failed": "failed",
    "skipped": "skipped",
    "upstream_failed": "upstream_failed",
}

# stdlib-only child: исполняется интерпретатором venv режима Б, dagprobe там не установлен.
# Результат — в файл (последний аргумент), не в stdout: логи Airflow мусорят его (уроки спайка).
MODE_B_CHILD = r"""
import importlib.util, json, os, sys, traceback
from datetime import datetime
from pathlib import Path

out_path = sys.argv[1]
try:
    from airflow.sdk.definitions.dag import DAG as SdkDAG

    dags_dir = Path(os.environ["AB_DAGS_DIR"])
    ids = [x for x in os.environ["AB_DAG_IDS"].split(",") if x]
    ld = datetime.fromisoformat(os.environ["AB_EXEC_DATE"])
    dags = {}
    for f in sorted(dags_dir.glob("*.py")):
        spec = importlib.util.spec_from_file_location(f.stem, f)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for obj in vars(module).values():
            if isinstance(obj, SdkDAG):
                dags[obj.dag_id] = obj
    missing = [i for i in ids if i not in dags]
    if missing:
        raise RuntimeError(f"live Airflow did not import DAGs: {missing}")
    result = {}
    for dag_id in ids:
        dr = dags[dag_id].test(logical_date=ld)
        result[dag_id] = {ti.task_id: str(ti.state) for ti in dr.get_task_instances()}
    payload = {"statuses": result, "error": ""}
except Exception:
    payload = {"statuses": {}, "error": traceback.format_exc()}
Path(out_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
"""


def _constraints_url(version: str, py: str) -> str:
    return (
        "https://raw.githubusercontent.com/apache/airflow"
        f"/constraints-{version}/constraints-{py}.txt"
    )


def _bin(venv: Path, name: str) -> Path:
    sub = "Scripts" if sys.platform == "win32" else "bin"
    return venv / sub / (f"{name}.exe" if sys.platform == "win32" else name)


def compare_statuses(
    a: dict[str, dict[str, str]], b: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    """Расхождения статусов A (шим) и B (живой Airflow); статусы A — в состояния Airflow."""
    findings: list[dict[str, Any]] = []
    for dag_id in sorted(set(a) | set(b)):
        for tid in sorted(set(a.get(dag_id, {})) | set(b.get(dag_id, {}))):
            va = _TO_AIRFLOW.get(a.get(dag_id, {}).get(tid, ""), a.get(dag_id, {}).get(tid))
            vb = b.get(dag_id, {}).get(tid)
            if va != vb:
                findings.append({"dag_id": dag_id, "task_id": tid, "in_a": va, "in_b": vb})
    return findings


def _run(cmd: list[str], env: dict[str, str]) -> None:
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed ({proc.returncode}): {proc.stderr[-2000:]}")


def _ensure_venv(venv: Path, version: str, py: str, env: dict[str, str]) -> Path:
    python = _bin(venv, "python")
    if python.exists():
        return python
    if not shutil.which("uv"):
        raise RuntimeError(
            f"venv {venv} не найден, а uv недоступен: создайте venv с apache-airflow=={version} "
            "самостоятельно и передайте его через --venv"
        )
    _run(["uv", "venv", "--python", py, str(venv)], env)
    _run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            f"apache-airflow=={version}",
            "--constraint",
            _constraints_url(version, py),
        ],
        env,
    )
    return python


def _mode_b(
    dags_folder: Path,
    dag_ids: list[str],
    home: Path,
    venv: Path,
    version: str,
    timeout_s: float,
) -> dict[str, dict[str, str]]:
    """Прогон живым Airflow: DAGS_FOLDER-синк + DAG.test() в venv режима Б."""
    sync = home / "dags-b"
    sync.mkdir(parents=True, exist_ok=True)
    for old in sync.glob("*.py"):
        old.unlink()
    for f in sorted(dags_folder.glob("*.py")):
        shutil.copy(f, sync / f.name)
    env = {
        **os.environ,
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__DAGS_FOLDER": str(sync),
        "AB_DAGS_DIR": str(sync),
        "AB_DAG_IDS": ",".join(dag_ids),
        "AB_EXEC_DATE": LOGICAL_DATE,
    }
    python = _ensure_venv(venv, version, "3.14", env)
    if not (home / "airflow.db").exists():
        _run([str(_bin(venv, "airflow")), "db", "migrate"], env)
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "live.json"
        proc = subprocess.run(
            [str(python), "-c", MODE_B_CHILD, str(out)],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        if not out.exists() or not out.stat().st_size:
            raise RuntimeError(f"live child failed (exit={proc.returncode}): {proc.stderr[-2000:]}")
        data = json.loads(out.read_text(encoding="utf-8"))
    if data.get("error"):
        raise RuntimeError(f"live child failed: {data['error']}")
    return cast(dict[str, dict[str, str]], data["statuses"])


def ab_command(
    dags_folder: Path,
    *,
    dag_id: str | None = None,
    fixtures: Path | None = None,
    venv: Path | None = None,
    airflow_version: str = "3.3.2",
    home: Path | None = None,
    timeout_s: float = LIVE_TIMEOUT_S,
) -> tuple[dict[str, object], int]:
    """Живой A/B: статусы шима против DAG.test() реального Airflow. Возврат (вывод, exit)."""
    dags_folder = Path(dags_folder)
    home = Path(home) if home else Path.cwd() / ".dagprobe"
    venv = Path(venv) if venv else home / "venv-mode-b"
    try:
        a: dict[str, dict[str, str]] = {}
        for f in sorted(dags_folder.glob("*.py")):
            result = analyze_file(f, timeout_s=timeout_s)
            for did in result.dag_ids:
                if dag_id and did != dag_id:
                    continue
                a[did] = run_dag(f, did, fixtures_path=fixtures, timeout_s=timeout_s).statuses
        if not a:
            return {
                "error": f"DAG {dag_id!r} not found in {dags_folder}"
                if dag_id
                else f"no DAGs in {dags_folder}"
            }, 2
        b = _mode_b(dags_folder, sorted(a), home, venv, airflow_version, timeout_s)
    except (TimeoutError, RuntimeError) as exc:
        return {"error": str(exc)}, 2
    findings = compare_statuses(a, b)
    return {"finding_count": len(findings), "findings": findings}, (1 if findings else 0)


def _emit_ab(out: dict[str, object], version: str) -> None:
    if out.get("finding_count") == 0:
        sys.stdout.write(f"no findings: shim matches live Airflow {version}\n")
        return
    lines = [f"live A/B: {out['finding_count']} расхождений(я) против Airflow {version}"]
    findings = cast("list[dict[str, Any]]", out.get("findings") or [])
    for fnd in findings:
        lines.append(
            f"DAG {fnd['dag_id']} / {fnd['task_id']}: {fnd['in_a']} (shim) → {fnd['in_b']} (live)"
        )
    sys.stdout.write("\n".join(lines) + "\n")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(ab_command(Path(sys.argv[1]))[1])
