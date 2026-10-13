"""Срез 1 (файл 18, Часть F): анализ DAG-файла через shim с изоляцией в subprocess.

``analyze_file`` — обёртка над наработками спайка (B2–B5): активирует shim,
импортирует DAG-файл и снимает граф. Сам файл исполняется в subprocess —
ошибка/зависание пользовательского кода не роняет процесс-родитель (§6.1).
Рендер template_fields — превью-фича §6.4 (Срез 2+), в AnalysisResult не входит.

Профиль пока один (v3.3) — выбор профиля появится вместе со вторым (файл 18, F4).
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

from dagprobe import errors

# ponytail: таймаут-константа без настройки (файл 18, F2); env-ручка — когда появятся висящие DAG
TIMEOUT_S = 30.0
LOGICAL_DATE = "2026-09-29T00:00:00+00:00"  # фиксированная logical_date превью/прогона

# скрипт, исполняемый в subprocess: снимок всех DAG файла в JSON на stdout.
# Внимание: сабпроцесс не наследует src-layout пакет неявно — запускаем
# [sys.executable, "-c", _CHILD, file]; dagprobe уже импортируем в родителе,
# значит при установленном пакете (uv sync / pip) он виден и ребёнку.
_CHILD = r"""
import importlib.util
import json
import re
import sys
import traceback

from dagprobe.shim import _core, activate, deactivate

path, out_path = sys.argv[1], sys.argv[3]
mode = sys.argv[2] if len(sys.argv) > 2 else "list"  # list | render (§6.4)
activate()

def emit(obj):
    # результат в файл, не в stdout: пользовательский код печатает при импорте
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, default=str)

try:
    spec = importlib.util.spec_from_file_location("dagprobe_target", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = {"dags": {}, "errors": []}
    if mode == "render":
        from datetime import datetime

        from dagprobe.shim import _templates
    for dag_id, dag in sorted(_core.all_dags().items()):
        entry = {
            "tasks": {
                t.task_id: {
                    "class": type(t).__name__,
                    "upstream": sorted(t.upstream_task_ids),
                    "downstream": sorted(t.downstream_task_ids),
                }
                for t in dag.task_dict.values()
            },
            "edges": dag.edges(),
        }
        if mode == "render":
            logical_date = datetime.fromisoformat("__LOGICAL_DATE__")
            rendered = {}
            for t in dag.task_dict.values():
                ctx = _templates.build_context(t, logical_date)
                # исходники шаблонов до рендера — для превью «до → после»
                before = {f: getattr(t, f, None) for f in getattr(t, "template_fields", ())}
                _templates.render_template_fields(t, ctx)
                rendered[t.task_id] = {
                    f: {"before": repr(v), "after": repr(getattr(t, f, None))}
                    for f, v in before.items()
                }
            entry["rendered"] = rendered
        result["dags"][dag_id] = entry
    emit(result)
except Exception:
    tb = traceback.format_exc()
    # локация в пользовательском коде: последняя рамка с его файлом (не <string>)
    frames = re.findall(rf'File "{re.escape(path)}", line (\d+)', tb)
    emit(
        {
            "dags": {},
            "errors": [
                {
                    "message": tb.strip().splitlines()[-1],
                    "traceback": tb,
                    "line": int(frames[-1]) if frames else None,
                }
            ],
        }
    )
finally:
    deactivate()
"""
_CHILD = _CHILD.replace("__LOGICAL_DATE__", LOGICAL_DATE)


@dataclasses.dataclass(frozen=True)
class DAGInfo:
    """Один DAG из файла: задачи, рёбра."""

    dag_id: str
    folder: str
    file: str
    tasks: dict[str, dict[str, object]]  # task_id -> {class, upstream, downstream}
    edges: list[list[str]]
    rendered: dict[str, dict[str, dict[str, str]]] = dataclasses.field(default_factory=dict)


def _error_from(raw: object, file: str) -> errors.DAGProbeError:
    """Ошибка из child-JSON (dict с message/line) или строка (exit/таймаут)."""
    if isinstance(raw, dict):
        return errors.DAGProbeError(
            code=errors.IMPORT_FAILURE,
            severity=errors.ERROR,
            message=str(raw.get("message", "")),
            file=file,
            line=cast("int | None", raw.get("line")),
        )
    return errors.DAGProbeError(
        code=errors.IMPORT_FAILURE, severity=errors.ERROR, message=str(raw), file=file
    )


@dataclasses.dataclass(frozen=True)
class AnalysisResult:
    """Модель вывода анализа (файл 02): dag_ids + типизированные ошибки."""

    dag_ids: list[str]
    errors: list[errors.DAGProbeError]
    import_duration_ms: float
    dags: list[DAGInfo]

    @classmethod
    def from_json(cls, data: dict[str, object], file: str) -> AnalysisResult:
        dags_data = cast(dict[str, dict[str, object]], data.get("dags") or {})
        dags = [
            DAGInfo(
                dag_id=str(dag_id),
                folder=str(dag_data["folder"]),
                file=str(dag_data["file"]),
                tasks=cast(dict[str, dict[str, object]], dag_data["tasks"]),
                edges=cast("list[list[str]]", dag_data["edges"]),
                rendered=cast(dict[str, dict[str, dict[str, str]]], dag_data.get("rendered") or {}),
            )
            for dag_id, dag_data in dags_data.items()
        ]
        return cls(
            dag_ids=sorted(dags_data),
            errors=[_error_from(e, file) for e in cast("list[object]", data.get("errors") or [])],
            import_duration_ms=float(cast("int | float", data.get("import_duration_ms", 0.0))),
            dags=dags,
        )


@dataclasses.dataclass(frozen=True)
class ChildRun:
    """Результат запуска child-скрипта: payload из файла либо диагностика."""

    payload: dict[str, object] | None
    timed_out: bool
    diagnostics: str


def run_child(child_src: str, args: list[str], *, timeout_s: float = TIMEOUT_S) -> ChildRun:
    """Запустить child и забрать его JSON-вывод из файла.

    Пользовательский DAG может печатать в stdout при импорте — парсить stdout
    нельзя (ложные ``IMPORT_FAILURE`` и падение родителя). Child пишет результат
    в отдельный файл, последним аргументом.
    """
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "child.json"
        cmd = [sys.executable, "-c", child_src, *args, str(out)]
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout_s, check=False
            )
        except subprocess.TimeoutExpired:
            return ChildRun(
                payload=None, timed_out=True, diagnostics=f"timeout after {timeout_s:.0f}s"
            )
        if out.exists() and out.stat().st_size:
            return ChildRun(
                payload=cast("dict[str, object]", json.loads(out.read_text(encoding="utf-8"))),
                timed_out=False,
                diagnostics="",
            )
        noise = (proc.stderr.strip() or proc.stdout.strip())[:2000]
        diag = f"exit={proc.returncode}: {noise}"
        return ChildRun(payload=None, timed_out=False, diagnostics=diag)


def _child_snapshot(data: dict[str, object], file: Path) -> dict[str, object]:
    """Дополнить снимок subprocess полями ``folder``/``file``, которые знает только родитель."""
    for dag_data in cast(dict[str, dict[str, object]], data.get("dags") or {}).values():
        dag_data["folder"] = str(file.parent)
        dag_data["file"] = file.name
    return data


def analyze_file(
    path: Path | str, *, timeout_s: float = TIMEOUT_S, render: bool = False
) -> AnalysisResult:
    """Проанализировать DAG-файл в изолированном subprocess с таймаутом.

    Ошибки импорта/парсинга — в ``errors`` (traceback строкой), не исключением:
    это нормальный результат анализа. Каждая задача — отдельный subprocess,
    изоляция как в §6.1. ``render=True`` добавляет рендер template_fields (§6.4).
    """
    file = Path(path).resolve()
    start = time.monotonic()
    run = run_child(_CHILD, [str(file), "render" if render else "list"], timeout_s=timeout_s)
    duration_ms = (time.monotonic() - start) * 1000

    if run.payload is None:
        code = errors.SANDBOX_TIMEOUT if run.timed_out else errors.IMPORT_FAILURE
        message = (
            f"timeout after {timeout_s:.0f}s importing {file.name}"
            if run.timed_out
            else run.diagnostics
        )
        return AnalysisResult(
            dag_ids=[],
            errors=[
                errors.DAGProbeError(
                    code=code, severity=errors.ERROR, message=message, file=file.name
                )
            ],
            import_duration_ms=duration_ms,
            dags=[],
        )
    data = _child_snapshot(run.payload, file)
    data["import_duration_ms"] = duration_ms
    return AnalysisResult.from_json(data, file.name)
