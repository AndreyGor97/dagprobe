"""Инспектор DAG для сравнения Mode A (shim) vs Mode B (реальный Airflow) — Часть C.

Запускается ОДИНАКОВО в обоих режимах (без if/else по режиму): строит из найденных
DAG машиночитаемый снимок — dag_id, задачи, рёбра, template_fields после рендера.
Вывод — JSON в stdout; сравнение — построчно вне этого скрипта.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

DAGS_DIR = Path(__file__).resolve().parent


def load_dag_modules(dags_dir: Path) -> list[Path]:
    return sorted(dags_dir.glob("*.py"))


def render_template(value: Any) -> Any:
    """Jinja-рендер строки-шаблона; не-строки возвращаются как есть.

    В Mode B используется нативный Airflow-рендер (task.render_template), сюда
    подставляется колбэком из режима.
    """
    return value


def snapshot_task(task: Any, render: Any) -> dict[str, Any]:
    """Снимок одной задачи: id, класс, template_fields (сырые и отрендеренные)."""
    fields = getattr(type(task), "template_fields", ()) or ()
    rendered: dict[str, Any] = {}
    for f in fields:
        raw = getattr(task, f, None)
        try:
            rendered[f] = render(raw) if isinstance(raw, str) else raw
        except Exception as exc:  # noqa: BLE001 — фиксируем расхождение, не роняя снимок
            rendered[f] = f"<render error: {exc}>"
    return {
        "task_id": task.task_id,
        "type": type(task).__name__,
        "template_fields": rendered,
    }


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "A"
    if mode == "B":
        # Настоящий Airflow: рендер — нативный (контекст задаёт сам Airflow)
        from airflow.utils.edge import add_edge  # noqa: F401  (маркер импорта airflow)

        def render(value: Any) -> Any:
            return value

        collect = collect_real
    else:
        from dagprobe.shim import activate

        activate()

        def render(value: Any) -> Any:
            return value  # спайк: Jinja-рендер Mode A — отдельный шаг (C1 уточняет)

        collect = collect_shim

    out = collect(render)
    json.dump(out, sys.stdout, indent=1, sort_keys=True, default=str)
    sys.stdout.write("\n")


def collect_shim(render: Any) -> dict[str, Any]:
    from dagprobe.shim import _core

    for path in load_dag_modules(DAGS_DIR):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    result: dict[str, Any] = {}
    for dag_id, dag in sorted(_core.all_dags().items()):
        result[dag_id] = {
            "tasks": sorted(dag.task_dict),
            "edges": dag.edges(),
            "task_details": [snapshot_task(t, render) for t in dag.task_dict.values()],
        }
    return result


def collect_real(render: Any) -> dict[str, Any]:
    from airflow.models.dag import DAG as RealDAG

    for path in load_dag_modules(DAGS_DIR):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    result: dict[str, Any] = dbag_snapshot(bag, render)
    return result


def dbag_snapshot(bag: Any, render: Any) -> dict[str, Any]:
    result: dict[str, AAB] = {}
    for dag_id, dag in sorted(bag.dags.items()):
        result[dag_id] = {
            "tasks": sorted(t.task_id for t in dag.tasks),
            "tasks2": sorted(dag.task_dict),
            "edges": sorted(
                (up, t.task_id) for t in dag.tasks for up in t.upstream_task_ids
            ),
            "tasks3": [snapshot_task(t, render) for t in dag.tasks],
        }
    return result


if __name__ == "__main__":
    main()
