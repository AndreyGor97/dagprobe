"""Jinja-рендер в шиме: контекст, макросы, фильтры, вложенные структуры (пост-спайк)."""

from __future__ import annotations

from datetime import UTC, datetime

from dagprobe.shim import _templates
from dagprobe.shim._core import BaseOperator
from dagprobe.shim._templates import (
    Macros,
    _AirflowEnvironment,
    build_context,
    render_template_fields,
)

LOGICAL_DATE = datetime(2026, 9, 29, tzinfo=UTC)


def _make_task() -> BaseOperator:
    """Настоящая задача шима: build_context требует task.dag/task.params."""
    from dagprobe.shim import _core

    with _core.DAG("t"):
        op = _core.BaseOperator(task_id="op")
    _core.reset_registry()
    return op


_TASK = _make_task()


def _render(template: str) -> str:
    ctx = build_context(_TASK, LOGICAL_DATE)
    return str(_templates._render(template, _AirflowEnvironment(cache_size=0), ctx))


def test_ds_and_macros() -> None:
    assert _render("{{ ds }}") == "2026-09-29"
    assert _render("{{ macros.ds_add(ds, 7) }}") == "2026-10-06"
    assert _render("{{ macros.ds_add(ds, -5) }}") == "2026-09-24"
    assert _render("{{ macros.ds_format(ds, '%Y-%m-%d', '%d.%m.%Y') }}") == "29.09.2026"


def test_context_keys() -> None:
    ctx = build_context(_TASK, LOGICAL_DATE)
    assert ctx["ds_nodash"] == "20260929"
    assert ctx["ts_nodash"] == "20260929T000000"
    assert ctx["ts"] == "2026-09-29T00:00:00+00:00"
    assert ctx["logical_date"] == LOGICAL_DATE


def test_filters() -> None:
    assert _render("{{ ds }} | {{ macros.ds_add(ds, 1) }}") == "2026-09-29 | 2026-09-30"
    # фильтры as-of-date, как FILTERS в templater.py airflow
    assert _render("{{ logical_date | ds }}") == "2026-09-29"
    assert _render("{{ logical_date | ds_nodash }}") == "20260929"
    assert _render("{{ logical_date | ts }}") == "2026-09-29T00:00:00+00:00"
    assert _render("{{ logical_date | ts_nodash }}") == "20260929T000000"


def test_nested_structures() -> None:
    out = _templates._render(
        {"cmd": "{{ ds }}", "args": ["{{ ds_nodash }}", ("{{ ts_nodash }}",)]},
        _AirflowEnvironment(cache_size=0),
        build_context(_TASK, LOGICAL_DATE),
    )
    assert out == {
        "cmd": "2026-09-29",
        "args": ["20260929", ("20260929T000000",)],
    }


def test_loop_and_statement() -> None:
    assert _render("{% for i in range(3) %}{{ i }}{% endfor %}") == "012"


def test_render_template_fields_on_operator() -> None:
    from dagprobe.shim import _core, activate, deactivate

    activate()
    try:
        with _core.DAG("d"):
            op = _core.BaseOperator(task_id="t")
            op.template_fields = ("bash_command",)
            op.bash_command = "{{ ds }}/{{ macros.ds_add(ds, 7) }}"
            render_template_fields(op, build_context(op, LOGICAL_DATE))
        assert op.bash_command == "2026-09-29/2026-10-06"
    finally:
        deactivate()


def test_uncovered_macro_fails_loud() -> None:
    try:
        _render("{{ macros.datetime_diff_for_humans(ds) }}")
    except Exception as exc:  # noqa: BLE001 — jinja заворачивает в TemplateRuntimeError
        assert "not implemented" in str(exc) or "datetime_diff_for_humans" in str(exc)
    else:
        raise AssertionError("непокрытый макрос должен падать громко")


def test_macros_laziness_and_identity() -> None:
    m = Macros()
    assert m.ds_add is m.ds_add  # _names — классовый словарь, стабилен между вызовами
    assert m.datetime is datetime
