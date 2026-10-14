"""CLI dagprobe: ``list`` / ``lint`` / ``render`` / ``run`` / ``audit`` / ``ab``.

Профиль версии для ``run``/``audit`` — встроенный выбор: ``3.3`` (умолчание)
и ``2.10`` (легаси-контекст, Срез 8); произвольная версия — файл 18, F4.

CI-режим (§6.21, Срез 6): у ``lint`` и ``run`` есть ``--format {text,json,junit}``,
``--fail-on {error,warning,never}``; у ``lint`` — ``--changed-only``/``--base-ref``
(только файлы из diff к базовой ветке). Exit-коды и JUnit — в ``dagprobe.ci``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import cast

from dagprobe import ci
from dagprobe.analyze import AnalysisResult, analyze_file
from dagprobe.audit import audit_2to3
from dagprobe.errors import DAGProbeError
from dagprobe.lint import lint_file
from dagprobe.live import _emit_ab, ab_command
from dagprobe.task_test import run_dag

FORMATS = ("text", "json", "junit")
GATES = ("error", "warning", "never")


def _collect(files: list[Path], *, timeout_s: float | None = None) -> AnalysisResult:
    """Импорт (§6.1) + линт (§6.3) по списку файлов — общий путь ``list`` и ``lint``."""
    errors: list[DAGProbeError] = []
    dags = []
    total_ms = 0.0
    for f in files:
        result = analyze_file(f) if timeout_s is None else analyze_file(f, timeout_s=timeout_s)
        errors.extend(ci.with_location(result.errors, f.name))
        dags.extend(result.dags)
        total_ms += result.import_duration_ms
        # линт (§6.3) — статический, без subprocess
        errors.extend(lint_file(f))
    return AnalysisResult(
        dag_ids=sorted(d.dag_id for d in dags),
        errors=errors,
        import_duration_ms=total_ms,
        dags=dags,
    )


def list_command(dags_folder: Path, *, timeout_s: float | None = None) -> AnalysisResult:
    """Проанализировать все ``*.py`` каталога, слить результаты в один."""
    return _collect(sorted(dags_folder.glob("*.py")), timeout_s=timeout_s)


def lint_command(
    dags_folder: Path, *, changed_only: bool = False, base_ref: str = "origin/main"
) -> tuple[AnalysisResult, list[Path]]:
    """Импорт+линт для CI: все ``*.py`` каталога или только изменённые (§6.21)."""
    files = (
        ci.changed_python_files(dags_folder, base_ref)
        if changed_only
        else sorted(dags_folder.glob("*.py"))
    )
    return _collect(files), files


def render_command(dags_folder: Path, *, dag_id: str | None = None) -> dict[str, object]:
    """Jinja Previewer (§6.4): рендер template_fields по задачам (before → after)."""
    out: dict[str, object] = {}
    for f in sorted(dags_folder.glob("*.py")):
        result = analyze_file(f, render=True)
        for dag in result.dags:
            if dag_id and dag.dag_id != dag_id:
                continue
            out[dag.dag_id] = dag.rendered
    return out


def run_command(
    dags_folder: Path,
    dag_id: str,
    *,
    task_ids: list[str] | None = None,
    fixtures: Path | None = None,
    fail_on: str = "error",
) -> tuple[dict[str, object], int]:
    """Task Test (§6.5): прогнать DAG/задачи на Fake-фикстурах. Возврат (вывод, exit)."""
    for f in sorted(dags_folder.glob("*.py")):
        result = analyze_file(f)
        if dag_id not in result.dag_ids:
            continue
        run = run_dag(f, dag_id, task_ids=task_ids, fixtures_path=fixtures)
        out: dict[str, object] = {
            "dag_id": dag_id,
            "statuses": run.statuses,
            "values": run.values,
            "failures": run.failures,
            "written_files": run.written,
        }
        return out, ci.exit_code(
            ci.findings_of(ci.run_cases(dag_id, run.statuses, run.failures)), fail_on=fail_on
        )
    return {"error": f"DAG {dag_id!r} not found in {dags_folder}"}, ci.USAGE


def _emit_lint(result: AnalysisResult, files: list[Path], fmt: str) -> None:
    """Вывод ``lint``: text (по умолчанию) / json / junit (§6.21)."""
    cases = ci.lint_cases(result.errors, files)
    if fmt == "junit":
        sys.stdout.write(ci.to_junit(cases, time_s=result.import_duration_ms / 1000) + "\n")
    elif fmt == "text":
        sys.stdout.write(ci.to_text(cases))
    else:
        json.dump(
            {
                "files": [f.name for f in files],
                "dag_ids": result.dag_ids,
                "errors": [e.to_dict() for e in result.errors],
                "import_duration_ms": round(result.import_duration_ms, 1),
            },
            sys.stdout,
            indent=1,
            ensure_ascii=False,
        )
        sys.stdout.write("\n")


def _emit_run(out: dict[str, object], fmt: str) -> None:
    """Вывод ``run``: json (по умолчанию, как в Срезах 3–5) / junit / text."""
    if fmt == "json":
        json.dump(out, sys.stdout, indent=1, ensure_ascii=False, default=str)
        sys.stdout.write("\n")
        return
    cases = ci.run_cases(
        str(out["dag_id"]),
        out["statuses"],  # type: ignore[arg-type]
        out["failures"],  # type: ignore[arg-type]
    )
    sys.stdout.write((ci.to_junit(cases) if fmt == "junit" else ci.to_text(cases)) + "\n")


def audit_command(
    dags_folder: Path,
    *,  # kind сейчас один — "2to3"; позиционный аргумент CLI приберегён под будущие
    dag_id: str | None = None,
    fixtures: Path | None = None,
    fail_on: str = "warning",
) -> tuple[dict[str, object], int]:
    """Аудит миграции (Срез 8): прогон 2.10 vs 3.3, находки-расхождения.

    Находка аудита по умолчанию валит exit 1 (fail_on="warning") — смысл команды
    в том, чтобы расхождение не прошло незамеченным. Возврат (вывод, exit).
    """
    try:
        findings = audit_2to3(dags_folder, dag_id=dag_id, fixtures=fixtures)
    except (TimeoutError, RuntimeError) as exc:
        return {"error": str(exc)}, ci.USAGE
    out: dict[str, object] = {"finding_count": len(findings), "findings": findings}
    return out, (1 if findings and fail_on != "never" else 0)


def _emit_audit(out: dict[str, object], fmt: str) -> None:
    """Вывод ``audit``: text (по умолчанию) / json (junit не имеет смысла — это не кейсы тестов)."""
    if fmt == "json":
        json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
        sys.stdout.write("\n")
        return
    findings = cast(list[dict[str, object]], out["findings"])
    if not findings:
        sys.stdout.write("no findings: behavior identical in 2.10 and 3.3\n")
        return
    lines: list[str] = []
    for fnd in findings:
        code = str(fnd["code"])
        if code == "status":
            lines.append(
                f"DAG {fnd['dag_id']} / {fnd['task_id']}: status {fnd['in_v2']} (2.10) "
                f"→ {fnd['in_v3']} (3.3)"
            )
            if fnd["detail"]:
                lines.append(f"  3.3: {fnd['detail']}")
        else:
            lines.append(
                f"DAG {fnd['dag_id']} / {fnd['task_id']}: value {fnd['in_v2']} (2.10) "
                f"→ {fnd['in_v3']} (3.3)"
            )
    sys.stdout.write("\n".join(lines) + "\n")


def _add_ci_flags(parser: argparse.ArgumentParser, *, default_format: str) -> None:
    parser.add_argument("--format", choices=FORMATS, default=default_format)
    parser.add_argument("--fail-on", choices=GATES, default="error")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dagprobe")
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list", help="list DAG ids in a folder")
    p_list.add_argument("--dags-folder", type=Path, required=True)
    p_list.add_argument("--fail-on", choices=GATES, default="error")
    p_lint = sub.add_parser("lint", help="import + lint a DAGs folder (CI gate)")
    p_lint.add_argument("--dags-folder", type=Path, required=True)
    p_lint.add_argument(
        "--changed-only", action="store_true", help="only files changed vs base branch"
    )
    p_lint.add_argument("--base-ref", default="origin/main", help="base ref for --changed-only")
    _add_ci_flags(p_lint, default_format="text")
    p_render = sub.add_parser("render", help="preview rendered template_fields (Jinja)")
    p_render.add_argument("--dags-folder", type=Path, required=True)
    p_render.add_argument("--dag-id", default=None)
    p_run = sub.add_parser("run", help="task test: run a DAG's tasks against fixtures")
    p_run.add_argument("--dags-folder", type=Path, required=True)
    p_run.add_argument("--dag-id", required=True)
    p_run.add_argument("--task-id", action="append", default=None)
    p_run.add_argument("--fixtures", type=Path, default=None)
    _add_ci_flags(p_run, default_format="json")
    p_audit = sub.add_parser("audit", help="behavior audit: run the DAG under 2.10 vs 3.3")
    p_audit.add_argument("kind", choices=["2to3"], help="migration pair (only 2to3 for now)")
    p_audit.add_argument("--dags-folder", type=Path, required=True)
    p_audit.add_argument("--dag-id", default=None)
    p_audit.add_argument("--fixtures", type=Path, default=None)
    p_audit.add_argument("--format", choices=("text", "json"), default="text")
    p_audit.add_argument("--fail-on", choices=GATES, default="warning")
    p_ab = sub.add_parser(
        "ab", help="live A/B: shim task statuses vs real Airflow DAG.test() (mode B)"
    )
    p_ab.add_argument("--dags-folder", type=Path, required=True)
    p_ab.add_argument("--dag-id", default=None)
    p_ab.add_argument(
        "--fixtures", type=Path, default=None, help="fixtures for the shim run (mode A only)"
    )
    p_ab.add_argument(
        "--venv",
        type=Path,
        default=None,
        help="existing mode-b venv (default: create .dagprobe/venv-mode-b via uv)",
    )
    p_ab.add_argument("--airflow-version", default="3.3.2")
    p_ab.add_argument(
        "--home",
        type=Path,
        default=None,
        help="AIRFLOW_HOME for the live run (default: ./.dagprobe)",
    )
    p_ab.add_argument("--format", choices=("text", "json"), default="text")

    args = parser.parse_args(argv)
    if args.command == "render":
        json.dump(
            render_command(args.dags_folder, dag_id=args.dag_id),
            sys.stdout,
            indent=1,
            ensure_ascii=False,
            default=str,
        )
        sys.stdout.write("\n")
        return 0
    if args.command == "run":
        out, code = run_command(
            args.dags_folder,
            args.dag_id,
            task_ids=args.task_id,
            fixtures=args.fixtures,
            fail_on=args.fail_on,
        )
        if "error" in out:  # прогон не состоялся — это не находка, а exit 2
            json.dump(out, sys.stdout, ensure_ascii=False)
            sys.stdout.write("\n")
            return code
        _emit_run(out, args.format)
        return code
    if args.command == "audit":
        out, code = audit_command(
            args.dags_folder,
            dag_id=args.dag_id,
            fixtures=args.fixtures,
            fail_on=args.fail_on,
        )
        if "error" in out:
            json.dump(out, sys.stdout, ensure_ascii=False)
            sys.stdout.write("\n")
            return code
        _emit_audit(out, args.format)
        return code
    if args.command == "ab":
        out, code = ab_command(
            args.dags_folder,
            dag_id=args.dag_id,
            fixtures=args.fixtures,
            venv=args.venv,
            airflow_version=args.airflow_version,
            home=args.home,
        )
        if "error" in out:
            json.dump(out, sys.stdout, ensure_ascii=False)
            sys.stdout.write("\n")
            return code
        if args.format == "json":
            json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
            sys.stdout.write("\n")
        else:
            _emit_ab(out, args.airflow_version)
        return code
    if args.command == "lint":
        try:
            result, files = lint_command(
                args.dags_folder, changed_only=args.changed_only, base_ref=args.base_ref
            )
        except ci.CiError as exc:  # нет git/базовой ветки — exit 2 с внятной причиной
            json.dump({"error": str(exc)}, sys.stdout, ensure_ascii=False)
            sys.stdout.write("\n")
            return ci.USAGE
        _emit_lint(result, files, args.format)
        return ci.exit_code(result.errors, fail_on=args.fail_on)
    if args.command == "list":
        result = list_command(args.dags_folder)
        json.dump(
            {
                "dag_ids": result.dag_ids,
                "errors": [e.to_dict() for e in result.errors],
                "import_duration_ms": round(result.import_duration_ms, 1),
            },
            sys.stdout,
            indent=1,
            ensure_ascii=False,
        )
        sys.stdout.write("\n")
        # gate-уровень настраиваемый (--fail-on), дефолт строгий — Срез 6 (§6.21)
        return ci.exit_code(result.errors, fail_on=args.fail_on)
    return ci.USAGE  # unreachable: subparsers required


if __name__ == "__main__":
    raise SystemExit(main())
