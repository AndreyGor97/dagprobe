"""Инспектор DAG для сравнения Mode A (shim) vs Mode B (реальный Airflow) — Часть C.

Запускается одинаково в обоих режимах: собирает из каждого DAG-файла снимок —
dag_id, отсортированные задачи, рёбра, классы задач. Вывод — JSON в stdout.
Лежит ВНЕ dags-каталога, чтобы DagBag его не парсил.

Использование:
    python inspect_dags.py A   # Mode A — активирует dagprobe shim
    python inspect_dags.py B   # Mode B — реальный airflow из окружения
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DAGS_DIR = Path(__file__).resolve().parent / "dags"

# фиксированная logical_date: контекст A/B должен быть идентичен, иначе сравниваем
# не рендер, а дату
LOGICAL_DATE = datetime(2026, 9, 29, tzinfo=UTC)


def load_files() -> None:
    for path in sorted(DAGS_DIR.glob("*.py")):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)


def kind(task: Any) -> str:
    """Семантический тип задачи, единый для обоих режимов (имена классов не сравниваем)."""
    name = type(task).__name__
    if getattr(task, "mapped", False) or name == "DecoratedMappedOperator":
        return "mapped"
    if getattr(task, "is_branch", False) or "BranchPython" in name:
        return "branch"
    if "Empty" in name:
        return "empty"
    if "Bash" in name:
        return "bash"
    if "Python" in name or "Decorated" in name:
        return "task"
    return name


def snapshot(dags: dict[str, Any]) -> dict[str, Any]:
    """Снимок всех DAG: единая структура для обоих режимов."""
    result: dict[str, Any] = {}
    for dag_id, dag in sorted(dags.items()):
        tasks: dict[str, Any] = dag.task_dict
        result[dag_id] = {
            "tasks": sorted(tasks),
            "edges": sorted((up, t.task_id) for t in tasks.values() for up in t.upstream_task_ids),
            "task_kinds": sorted(
                ({"task_id": t.task_id, "kind": kind(t)} for t in tasks.values()),
                key=lambda d: d["task_id"],
            ),
            "rendered": render_all(dag),
        }
    return result


def render_all(dag: Any) -> dict[str, str]:
    """Отрендерить template_fields каждой задачи с одинаковым контекстом A/B."""
    out: dict[str, str] = {}
    for task_id, task in sorted(dag.task_dict.items()):
        for field in getattr(task, "template_fields", ()):
            value = getattr(task, field, None)
            if isinstance(value, str) and ("{{" in value or "{%" in value):
                out[f"{task_id}.{field}"] = render_one(task, value)
    return out


def render_one(task: Any, value: str) -> str:
    """Рендер одной строки: Mode A — shim._templates, Mode B — настоящий airflow."""
    if MODE == "A":
        from dagprobe.shim import _templates

        ctx = _templates.build_context(task, LOGICAL_DATE)
        return str(_templates._render(value, _templates._AirflowEnvironment(cache_size=0), ctx))
    ctx = {
        "ds": LOGICAL_DATE.strftime("%Y-%m-%d"),
        "ds_nodash": LOGICAL_DATE.strftime("%Y%m%d"),
        "ts": LOGICAL_DATE.isoformat(),
        "ts_nodash": LOGICAL_DATE.strftime("%Y%m%dT%H%M%S"),
        "ts_nodash_with_tz": LOGICAL_DATE.isoformat().replace("-", "").replace(":", ""),
        "logical_date": LOGICAL_DATE,
        "macros": _B_Macros(),
        "task": task,
        "dag": task.dag,
        "params": getattr(task, "params", {}) or {},
    }
    from airflow.sdk.definitions._internal.templater import SandboxedEnvironment

    return SandboxedEnvironment(cache_size=0).from_string(value).render(ctx)


class _B_Macros:
    """macros для Mode B — ленивый доступ к настоящим макросам airflow."""

    def __getattr__(self, item: str) -> Any:
        import airflow.sdk.execution_time.macros as m

        return getattr(m, item)

    # jinja-sandbox не даёт вызвать __getattr__-прокси без этих флагов
    unsafe_callable = False
    alters_data = False


MODE = "A"


def main() -> None:
    global MODE
    mode = sys.argv[1] if len(sys.argv) > 1 else "A"
    MODE = mode
    if mode == "B":
        load_files()  # DAG-файлы регистрируются в глобальном реестре настоящего Airflow
        from airflow.dag_processing.dagbag import DagBag

        dags = dict(DagBag(dag_folder=str(DAGS_DIR)).dags)
    else:
        from dagprobe.shim import _core, activate

        activate()
        load_files()
        dags = dict(_core.all_dags())

    json.dump(snapshot(dags), sys.stdout, indent=1, sort_keys=True, default=str)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
