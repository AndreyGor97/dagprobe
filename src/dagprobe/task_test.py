"""Task Test (§6.5, Срез 3): исполнение задач DAG на Fake-фикстурах.

Прогон идёт в том же subprocess-паттерне, что analyze (изоляция §6.1): child
импортирует DAG-файл, активирует RunContext+фикстуры, исполняет задачи в
топологическом порядке, рендерит template_fields перед вызовом, кладёт
результаты в XCom. Родитель получает JSON: статусы задач, XCom-значения, ошибки.

Срез 7: BranchPythonOperator выбирает ветки (невыбранные direct-downstream →
skipped), downstream решается по trigger_rule с семантикой Airflow 3.3.2
(срисовано с ``ti_deps/deps/trigger_rule_dep.py``). Статусы: ok, failed,
skipped, upstream_failed — как в Airflow.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any, cast

from dagprobe.analyze import LOGICAL_DATE, TIMEOUT_S, run_child

_RUN_CHILD_TEMPLATE = r"""
import importlib.util
import json
import sys
import traceback

from dagprobe.shim import _core, _fileintercept, _intercept, activate, deactivate
from dagprobe.shim import _templates, xcom as xcom_mod
from dagprobe.shim.airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    ShortCircuitOperator,
)
from dagprobe.task_test import branch_targets, trigger_rule_decide

path, run_json, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
_raw_open = open  # до activate(): запись результата прогона не под файловым стабом
cfg = json.loads(run_json)


def emit(obj):
    # результат в файл, не в stdout: тело задачи может печатать (как в analyze);
    # _raw_open захвачен до активации фикстур — файловый стаб не съедает вывод прогона
    with _raw_open(out_path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, default=str)

def resolve(v):
    # XComArg → значение из XCom; списки — поэлементно
    from dagprobe.shim._taskflow import XComArg

    if isinstance(v, XComArg):
        return xcom_mod.xcom.get(v.task.task_id, v.key or "return_value")
    if isinstance(v, list):
        return [resolve(x) for x in v]
    if isinstance(v, dict):
        return {k: resolve(x) for k, x in v.items()}
    return v


def _run_mapped(task, args, kwargs):
    # Dynamic task mapping (.expand()/.expand_kwargs()): callable — раз на элемент
    # expand-аргументов; результат задачи — список результатов раскруток
    # (downstream видит список, как в Airflow).
    xk = kwargs.pop("__expand_kwargs__", None)
    static = {k: v for k, v in kwargs.items() if not k.startswith("__mapped_")}
    if xk is not None:
        # expand_kwargs: каждый dict — полный набор kwargs одной раскрутки
        if not isinstance(xk, list) or not all(isinstance(d, dict) for d in xk):
            raise ValueError(
                f"task {task.task_id!r}: expand_kwargs expects a list of dicts "
                f"(or an XComArg resolving to one), got {type(xk).__name__}"
            )
        results, errs = [], []
        for i, call_kwargs in enumerate(xk):
            try:
                results.append(task.python_callable(*args, **{**static, **call_kwargs}))
            except Exception as e:
                # раскрутки независимы (как TIs в Airflow): падение индекса не мешает остальным
                errs.append(f"map_index={i}: {''.join(traceback.format_exception(e)).strip()}")
        return results, errs
    mapped = {k[9:]: v for k, v in kwargs.items() if k.startswith("__mapped_")}
    if not mapped:
        raise ValueError(f"mapped task {task.task_id!r} has no expand arguments")
    lengths = {len(v) for v in mapped.values()}
    if len(lengths) > 1:
        raise ValueError(
            f"task {task.task_id!r}: expand arguments have different lengths {sorted(lengths)}"
        )
    results, errs = [], []
    for i in range(lengths.pop()):
        call_kwargs = {**static, **{k: v[i] for k, v in mapped.items()}}
        inner = call_kwargs.pop("op_kwargs", None)
        if isinstance(inner, dict):
            # классические операторы: expand(op_kwargs=[{...}]) — dict одной раскрутки
            # перекрывает static (как в Airflow: mapped выигрывает у partial)
            call_kwargs.update(inner)
        try:
            results.append(task.python_callable(*args, **call_kwargs))
        except Exception as e:
            errs.append(f"map_index={i}: {''.join(traceback.format_exception(e)).strip()}")
    return results, errs


def topo_order(dag):
    # Кан-подобный обход: задача готова, когда все upstream исполнены/пропущены
    done, order = set(), []
    pending = list(dag.task_dict)
    while pending:
        progressed = False
        for tid in list(pending):
            ups = dag.task_dict[tid].upstream_task_ids
            if ups <= done:
                order.append(tid)
                done.add(tid)
                pending.remove(tid)
                progressed = True
        if not progressed:
            raise RuntimeError(f"cycle in dag {dag.dag_id!r}")
    return order

activate()
try:
    # Фикстуры ставятся ДО импорта DAG: чтение файла на уровне модуля (частый паттерн)
    # тоже должно попасть под стаб (§8.7), иначе прогон молча пойдёт на реальную ФС.
    from pathlib import Path as P

    from dagprobe.fixtures import Fixtures

    if cfg.get("fixtures"):
        _intercept.set_fixtures(Fixtures.load(P(cfg["fixtures"])))
    else:
        _intercept.set_fixtures(Fixtures())  # пустые: любой conn_id → громкая ошибка

    spec = importlib.util.spec_from_file_location("dagprobe_target", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    from datetime import datetime

    logical_date = datetime.fromisoformat("__LOGICAL_DATE__")

    dag = _core.all_dags()[cfg["dag_id"]]
    statuses, values, failures = {}, {}, {}
    order = topo_order(dag)
    subset = cfg.get("task_ids")
    for tid in order:
        if subset and tid not in subset:
            continue  # подмножество задач: триггер-правила не считаем (как airflow tasks test -t)
        if tid in statuses:
            continue  # статус уже решён каскадом (skip-ветка / не выполнено правило)
        task = dag.task_dict[tid]
        if task.upstream_task_ids:
            verdict = trigger_rule_decide(
                str(getattr(task, "trigger_rule", "all_success")),
                [statuses[u] for u in task.upstream_task_ids],
            )
            if verdict != "run":
                statuses[tid] = verdict
                continue
        xcom_mod.RunContext.activate(cfg["run_id"])
        try:
            # рендер внутри RunContext (как в Airflow: template_fields рендерятся
            # при старте задачи) — иначе xcom_pull в шаблоне громко падает;
            # ошибка рендера → failed этой задачи, не <setup> всего прогона
            _templates.render_template_fields(
                task, _templates.build_context(task, logical_date, cfg.get("profile", "3.3"))
            )
            if getattr(task, "python_callable", None) is not None:
                # аргументы-XComArg заменяются на фактические значения XCom
                args = [resolve(a) for a in getattr(task, "op_args", ())]
                kwargs = {k: resolve(v) for k, v in getattr(task, "op_kwargs", {}).items()}
                ctx = _templates.build_context(task, logical_date, cfg.get("profile", "3.3"))
                with _templates.current_context(ctx):
                    if getattr(task, "mapped", False):
                        result, errs = _run_mapped(task, args, kwargs)
                        if not result and not errs:
                            # как в Airflow: 0 раскруток → задача skipped, downstream за ней тоже
                            statuses[tid] = "skipped"
                            continue
                    else:
                        result = task.python_callable(*args, **kwargs)
                        errs = None
                xcom_mod.xcom.set(tid, "return_value", result)
                values[tid] = repr(result)
                if errs:
                    # как в Airflow: задача failed, но частичные успехи уже в XCom
                    # (downstream с all_done вытянет список успешных раскруток)
                    raise RuntimeError("\n".join(errs))
                statuses[tid] = "ok"
                if isinstance(task, BranchPythonOperator):
                    # семантика BranchMixIn (airflow 3.3.2): None → все downstream skipped,
                    # невыбранные direct-downstream → skipped, каскад решат trigger_rule
                    _chosen, skipped = branch_targets(task, result)
                    for s in skipped:
                        statuses[s] = "skipped"
                elif isinstance(task, ShortCircuitOperator) and not result:
                    # ShortCircuitOperator (airflow 3.3.2): falsy → скип downstream.
                    # ignore_downstream_trigger_rules=True (дефолт): всё поддерево,
                    # trigger_rule не учитываются; False — только прямые наследники.
                    frontier = [str(task.task_id)]
                    seen: set[str] = set()
                    while frontier:
                        cur = dag.task_dict[frontier.pop()]
                        for s in cur.downstream_task_ids:
                            if s in seen:
                                continue
                            seen.add(s)
                            statuses[s] = "skipped"
                            if getattr(task, "ignore_downstream_trigger_rules", True):
                                frontier.append(s)
            else:
                statuses[tid] = "skipped"  # не-Python задачи прогон не исполняет
        except Exception as e:
            statuses[tid] = "failed"
            failures[tid] = "".join(traceback.format_exception(e)).strip()
        finally:
            xcom_mod.RunContext.deactivate()
    emit(
        {
            "statuses": statuses,
            "values": values,
            "failures": failures,
            "written": _fileintercept.written(),
        }
    )
except Exception:
    emit(
        {
            "statuses": {},
            "values": {},
            "failures": {"<setup>": traceback.format_exc()},
            "written": {},
        }
    )
finally:
    deactivate()


"""
_RUN_CHILD = _RUN_CHILD_TEMPLATE.replace("__LOGICAL_DATE__", LOGICAL_DATE)


def branch_targets(task: Any, result: Any) -> tuple[list[str], list[str]]:
    """Разбор ответа брэнча: (выбранные task_id, пропускаемые direct-downstream).

    Семантика BranchMixIn (airflow 3.3.2): None → выбрать пусто (все downstream
    skipped); str → одна ветка; иной не-итерируемый → одна «ветка». Чужой
    task_id (не downstream) — громкая ошибка: Airflow это молча проглотил бы,
    но мы инструмент отладки, тишина здесь = потерянная ветка без объяснений.
    """
    chosen = [result] if isinstance(result, str) else list(result or ())
    chosen = [str(c) for c in chosen]
    downs = task.downstream_task_ids
    for c in chosen:
        if c not in downs:
            msg = (
                f"BranchPythonOperator {task.task_id!r}: returned {c!r}, "
                f"which is not a downstream task (downstream: {sorted(downs)})"
            )
            raise ValueError(msg)
    return chosen, sorted(downs - set(chosen))


def trigger_rule_decide(rule: str, ups: list[str]) -> str:
    """Вердикт «run»/«skipped»/«upstream_failed» по статусам upstream.

    Таблица срисована с airflow/ti_deps/deps/trigger_rule_dep.py (3.3.2,
    ветка flag_upstream_failed): невыполненное правило ставит upstream_failed,
    кроме случаев, где Airflow сам ставит SKIPPED (полностью скипнутые upstream
    и т.п.). Статусы upstream: ok / failed / skipped / upstream_failed.
    """
    s, f, k, uf = (ups.count(x) for x in ("ok", "failed", "skipped", "upstream_failed"))
    n = len(ups)
    if rule == "always":
        return "run"
    if rule == "all_success":
        return "run" if s == n else ("upstream_failed" if f + uf else "skipped")
    if rule == "all_failed":
        return "run" if s == 0 and k == 0 and n else "skipped"
    if rule == "all_done":
        return "run"  # все upstream уже завершились (топологический прогон)
    if rule == "one_success":
        if s >= 1:
            return "run"
        return "skipped" if k == n else "upstream_failed"
    if rule == "one_failed":
        return "run" if f + uf >= 1 else "skipped"
    if rule == "one_done":
        return "run" if s + f >= 1 else "skipped"  # uf не считается — как в Airflow
    if rule == "none_failed":
        return "run" if f + uf == 0 else "upstream_failed"
    if rule == "none_failed_min_one_success":
        if f + uf:
            return "upstream_failed"
        return "run" if s >= 1 else ("skipped" if k == n else "upstream_failed")
    if rule == "none_skipped":
        return "run" if k == 0 else "skipped"
    if rule == "all_skipped":
        return "run" if k == n else "skipped"
    if rule == "all_done_min_one_success":
        if k:
            return "skipped"  # скипнутые не считаются «done» для этого правила
        return "run" if s >= 1 else "upstream_failed"
    msg = f"unknown trigger_rule {rule!r} — Airflow does not know it (typo?)"
    raise ValueError(msg)


@dataclasses.dataclass(frozen=True)
class TaskRunResult:
    """Результат прогона: статус задачи → значение/ошибка + «записанные» файлы (§8.7)."""

    statuses: dict[str, str]
    values: dict[str, str]
    failures: dict[str, str]
    written: dict[str, str] = dataclasses.field(default_factory=dict)


def run_dag(
    path: Path | str,
    dag_id: str,
    *,
    task_ids: list[str] | None = None,
    fixtures_path: Path | str | None = None,
    run_id: str = "dagprobe",
    timeout_s: float = TIMEOUT_S,
    profile: str = "3.3",
) -> TaskRunResult:
    """Прогнать DAG (или подмножество задач) на Fake-фикстурах в subprocess.

    ``profile`` — версионный профиль контекста рендера: ``"3.3"`` (умолчание)
    или ``"2.10"`` (легаси-ключи контекста, Срез 8).
    """
    file = Path(path).resolve()
    cfg = json.dumps(
        {
            "dag_id": dag_id,
            "task_ids": task_ids,
            "fixtures": str(Path(fixtures_path).resolve()) if fixtures_path else None,
            "run_id": run_id,
            "profile": profile,
        }
    )
    run = run_child(_RUN_CHILD, [str(file), cfg], timeout_s=timeout_s)
    if run.payload is None:
        msg = f"timeout after {timeout_s:.0f}s running {dag_id}"
        raise (TimeoutError(msg) if run.timed_out else RuntimeError(run.diagnostics))
    data = run.payload
    return TaskRunResult(
        statuses=cast(dict[str, str], data.get("statuses") or {}),
        values=cast(dict[str, str], data.get("values") or {}),
        failures=cast(dict[str, str], data.get("failures") or {}),
        written=cast(dict[str, str], data.get("written") or {}),
    )
