"""A/B-сверка статусов прогона: shim (Mode A) против честного Airflow 3.3.2 (Mode B).

Срез 7: BranchPythonOperator (выбор ветки, skip невыбранных) и trigger_rule
(all_failed за скипнутым, каскад upstream_failed, one_failed/always).

Запуск — по образцу inspect_dags.py (файл DAG один и тот же, venv разные):

    uv run python spike/run_ab.py A > spike/out/run_a.json
    AIRFLOW_HOME=$PWD/.airflow-b .venv-mode-b/bin/python spike/run_ab.py B > spike/out/run_b.json
    .venv/bin/python spike/run_ab.py compare   # exit 1 при расхождении
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

EXEC_DATE = datetime(2026, 1, 1, tzinfo=UTC)

SOURCES = {
    "ab_branch": """
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG

with DAG("ab_branch", schedule=None, start_date=None, catchup=False) as dag:

    def route(**_):
        return "a"

    route_t = BranchPythonOperator(task_id="route", python_callable=route)
    a = PythonOperator(task_id="a", python_callable=lambda **_: 1)
    b = PythonOperator(task_id="b", python_callable=lambda **_: 2, trigger_rule="all_failed")
    c = PythonOperator(task_id="c", python_callable=lambda **_: 3, trigger_rule="all_failed")
    rep = PythonOperator(task_id="rep", python_callable=lambda **_: 4)

    route_t >> [a, b]
    b >> c
    a >> rep
""",
    "ab_cascade": """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("ab_cascade", schedule=None, start_date=None, catchup=False) as dag:

    def boom(**_):
        raise ValueError("нарочно")

    t1 = PythonOperator(task_id="t1", python_callable=boom)
    t2 = PythonOperator(task_id="t2", python_callable=lambda **_: 1)
    t3 = PythonOperator(task_id="t3", python_callable=lambda **_: 1, trigger_rule="none_failed")
    t4 = PythonOperator(task_id="t4", python_callable=lambda **_: 1, trigger_rule="always")
    t5 = PythonOperator(task_id="t5", python_callable=lambda **_: 1, trigger_rule="one_failed")

    t1 >> t2 >> t3
    t1 >> t4
    t1 >> t5
""",
}

# статус shim → состояние Airflow
_TO_B = {
    "ok": "success",
    "failed": "failed",
    "skipped": "skipped",
    "upstream_failed": "upstream_failed",
}


def write_dags(d: Path) -> list[Path]:
    # по файлу на DAG: в одном модуле `with DAG(...) as dag:` одну и ту же переменную
    files = []
    for dag_id, src in SOURCES.items():
        file = d / f"{dag_id}.py"
        file.write_text(src, encoding="utf-8")
        files.append(file)
    return files


def mode_a() -> dict[str, dict[str, str]]:
    from dagprobe.task_test import run_dag

    out = {}
    with tempfile.TemporaryDirectory() as d:
        files = write_dags(Path(d))
        for file in files:
            dag_id = file.stem  # имя файла = dag_id (см. write_dags)
            out[dag_id] = run_dag(file, dag_id).statuses
    return out


def mode_b() -> dict[str, dict[str, str]]:
    # DAG.test() в 3.3.2 синхронизирует DAG в БД через бандл dags-folder: файл обязан
    # лежать в AIRFLOW__CORE__DAGS_FOLDER (его задаёт запускающий, см. ci.yml)
    dags_dir = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", Path(__file__).parent / "ab_dags"))
    dags_dir.mkdir(parents=True, exist_ok=True)
    files = write_dags(dags_dir)
    dags = {}
    for f in files:
        spec = importlib.util.spec_from_file_location(f.stem, f)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from airflow.sdk.definitions.dag import DAG as SdkDAG

        dags.update({o.dag_id: o for o in vars(module).values() if isinstance(o, SdkDAG)})
    out = {}
    for dag_id in SOURCES:
        dr = dags[dag_id].test(logical_date=EXEC_DATE)
        assert dr is not None, f"{dag_id}: DAG.test() не вернул DagRun"
        out[dag_id] = {ti.task_id: str(ti.state) for ti in dr.get_task_instances()}
    return out


def _load(path: str) -> dict:
    # stdout режима B замусорен логами airflow (в них есть "{") — JSON начинается
    # со строки, целиком начинающейся с "{"
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith("{"))
    return json.loads("\n".join(lines[start:]))


def compare() -> int:
    a, b = _load("spike/out/run_a.json"), _load("spike/out/run_b.json")
    ok = True
    for dag_id in SOURCES:
        for tid in sorted(a[dag_id]):
            va, vb = _TO_B[a[dag_id][tid]], b[dag_id][tid]
            mark = "OK " if va == vb else "!!!"
            if va != vb:
                ok = False
            print(f"{mark} {dag_id}.{tid}: A={va} B={vb}")
    print("A/B: статусы совпадают" if ok else "A/B: ЕСТЬ РАСХОЖДЕНИЯ")
    return 0 if ok else 1


def main() -> int:
    mode = sys.argv[1]
    if mode == "A":
        print(json.dumps(mode_a(), ensure_ascii=False, indent=1))
    elif mode == "B":
        print(json.dumps(mode_b(), ensure_ascii=False, indent=1))
    else:
        return compare()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
