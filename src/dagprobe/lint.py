"""Линтер DAG-кода (§6.3) — базовые правила на AST, без исполнения кода.

Срез 2: три правила из §6.3, которые ловятся статически и не требуют
поведенческого прогона: deprecated API под целевую версию, недетерминированное
время, неявный catchup. Остальные правила §6.3 (сенсоры без timeout, XCom
объёма, top-level side-эффекты) — по мере добавления: сенсоры требуют знания
классов операторов, side-эффекты — поведенческий слой.

Инлайн-отключение `# dagprobe: ignore=<rule>` — принцип §6.3, реализован сразу.
"""

from __future__ import annotations

import ast
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from dagprobe.errors import (
    DEPRECATED_API,
    IMPLICIT_CATCHUP,
    LAZY_IMPORT,
    NONDETERMINISTIC_TIME,
    WARNING,
    DAGProbeError,
)

# ponytail: реестр правил — плоский dict, никакой иерархии классов правил,
# пока правил меньше ~10; система off/warn/error-конфигурации — Срез 6
Rule = Callable[[Path, ast.Module], list[DAGProbeError]]

# deprecated API: старое имя → (новое, с какой версии Airflow удалено/изменено)
_DEPRECATED = {
    "schedule_interval": ("schedule", "Airflow 3"),
    "execution_date": ("logical_date", "Airflow 3"),
}


def _ignored(node: ast.AST, lines: dict[int, str]) -> set[str]:
    """Коды, отключённые инлайн-комментарием `# dagprobe: ignore=CODE[,CODE]`."""
    codes: set[str] = set()
    last_line = getattr(node, "end_lineno", None) or getattr(node, "lineno", None)
    if isinstance(last_line, int) and last_line in lines:
        for part in lines[last_line].split("ignore=", 1)[-1].split(","):
            code = part.strip().rstrip("*/# ").strip()
            if code:
                codes.add(code)
    return codes


def rule_deprecated_api(_path: Path, tree: ast.Module) -> list[DAGProbeError]:
    """`schedule_interval`/`execution_date` как kwargs DAG/операторов — deprecated."""
    out: list[DAGProbeError] = []
    for node in ast.walk(tree):
        ignored = DEPRECATED_API in _ignored(node, _lines)
        if isinstance(node, ast.keyword) and node.arg in _DEPRECATED and not ignored:
            new, since = _DEPRECATED[node.arg]
            out.append(
                DAGProbeError(
                    code=DEPRECATED_API,
                    severity=WARNING,
                    message=f"{node.arg}= is deprecated since {since}; use {new}=",
                    line=node.lineno,
                )
            )
    return out


def rule_nondeterministic_time(_path: Path, tree: ast.Module) -> list[DAGProbeError]:
    """`datetime.now()`/`utcnow()` в коде DAG — недетерминированное время.

    Правильный паттерн — `airflow.utils.dates.days_ago` или параметризация
    через start_date;.now() в теле DAG меняет поведение при каждом парсинге.
    """
    out: list[DAGProbeError] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("now", "utcnow")
            and NONDETERMINISTIC_TIME not in _ignored(node, _lines)
        ):
            out.append(
                DAGProbeError(
                    code=NONDETERMINISTIC_TIME,
                    severity=WARNING,
                    message=f".{node.func.attr}() in DAG code is non-deterministic; "
                    "use airflow.utils.dates.days_ago or a fixed start_date",
                    line=node.lineno,
                )
            )
    return out


def rule_implicit_catchup(_path: Path, tree: ast.Module) -> list[DAGProbeError]:
    """DAG без явного `catchup=` — унаследует глобальный дефолт (Airflow 3: True).

    С implicit catchup и старым start_date первый запуск догонит все пропущенные
    периоды — риск backfill-взрыва (§6.3).
    """
    out: list[DAGProbeError] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "DAG":
            kwargs = {k.arg for k in node.keywords}
            if "catchup" not in kwargs and IMPLICIT_CATCHUP not in _ignored(node, _lines):
                out.append(
                    DAGProbeError(
                        code=IMPLICIT_CATCHUP,
                        severity=WARNING,
                        message="DAG without explicit catchup= — backfill explosion risk with an old start_date",  # noqa: E501
                        line=node.lineno,
                    )
                )
    return out


def rule_lazy_import(_path: Path, tree: ast.Module) -> list[DAGProbeError]:
    """`lazy import` (PEP 810, Python 3.15+) в DAG-файле — warning.

    Отложенный импорт прячет import-ошибку до первого обращения: `dagprobe list`
    покажет файл зелёным, а упадёт задача на воркере. Воркер на Python < 3.15
    не разберёт весь файл (SyntaxError). Правило активно только там, где синтаксис
    парсится (3.15+); на нижних версиях то же место ловит analyze_file как
    IMPORT_FAILURE.
    """
    out: list[DAGProbeError] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.Import, ast.ImportFrom))
            and getattr(node, "is_lazy", False)  # поле есть только в AST 3.15+
            and LAZY_IMPORT not in _ignored(node, _lines)
        ):
            out.append(
                DAGProbeError(
                    code=LAZY_IMPORT,
                    severity=WARNING,
                    message="lazy import (PEP 810): import errors surface at first use, "
                    "not at DAG import; workers on Python < 3.15 fail with SyntaxError",
                    line=node.lineno,
                )
            )
    return out


RULES: dict[str, Rule] = {
    DEPRECATED_API: rule_deprecated_api,
    NONDETERMINISTIC_TIME: rule_nondeterministic_time,
    IMPLICIT_CATCHUP: rule_implicit_catchup,
    LAZY_IMPORT: rule_lazy_import,
}


_lines: dict[int, str] = {}  # lineno → исходная строка; обновляется в lint_file


def lint_file(path: Path) -> list[DAGProbeError]:
    """Прогнать все правила по файлу; ошибки парсинга — не наша зона (analyze_file)."""
    global _lines
    source = path.read_text(encoding="utf-8")
    _lines = dict(enumerate(source.splitlines(), 1))
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []  # синтаксис уже пойман analyze_file как IMPORT_FAILURE
    out: list[DAGProbeError] = []
    for rule in RULES.values():
        out.extend(rule(path, tree))
    return [replace(e, file=path.name) for e in out]
