"""Timetable-эмуляция легаси-ключей ``prev_*``/``next_*`` профиля 2.10.

Семантика сверена с живым 2.10.5 (зонд Срез 18+, taskinstance.py:1032-1075):
``prev_execution_date = dag.previous_schedule(logical_date)`` — последний тик
строго до ``logical_date``; ``next_execution_date`` = ``logical_date``
следующего интервала = ``data_interval_end`` текущего — последний тик
**не позже** ``logical_date`` (вне сетки совпадает с prev, на сетке — сам
``logical_date``). Расписания нет (``None``/``@once``) → ``(None, None)`` —
jinja рендерит как ``None``, ровно как живой 2.10. Расчёт честный stdlib
(croniter в dagprobe не ставим — решение пользователя): cron-выражение
разбирается в множества значений полей, момент ищется перебором дней и
минут; сверка с croniter — ``spike/check_timetable.py``. Имена дней/месяцев
(MON/JAN) поддержаны; остальное неподдерживаемое — громко (``ValueError``),
не тихий ноль (принцип Срез 8).
"""

from __future__ import annotations

from datetime import datetime, timedelta

_CRON_ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

_MAX_DAYS = 366 * 20  # предел перебора дней: громкая ошибка вместо вечного цикла

_MONTH_NAMES = {
    v: i + 1
    for i, v in enumerate(
        ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
    )
}
_DOW_NAMES = {v: i for i, v in enumerate(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"])}


def _num(token: str, names: dict[str, int], field: str) -> int:
    if names:
        name = token.strip().upper()[:3]
        if name in names:
            return names[name]
    if not token.isdigit():
        msg = f"cron field {field!r}: value {token!r} is not supported (numbers only)"
        raise ValueError(msg)
    return int(token)


def _parse_field(field: str, low: int, high: int, names: dict[str, int]) -> set[int]:
    """Множество допустимых значений поля cron: списки, диапазоны, ``*/шаг``."""
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        body = part
        if "/" in part:
            body, step_s = part.split("/", 1)
            step = int(step_s)
            if step <= 0:
                msg = f"cron field {field!r}: step must be positive"
                raise ValueError(msg)
        if body == "*":
            values.update(range(low, high + 1, step))
        elif "-" in body:
            a_s, b_s = body.split("-", 1)
            a, b = _num(a_s, names, field), _num(b_s, names, field)
            values.update(range(a, b + 1, step))
        else:
            n = _num(body, names, field)
            if "/" in part:  # "5/15" = 5, 20, 35, ... до high (как в croniter)
                values.update(range(n, high + 1, step))
            else:
                values.add(n)
    if not values:
        msg = f"cron field {field!r}: no values in range [{low}..{high}]"
        raise ValueError(msg)
    return values


# (минуты, часы, dom, месяц, dow, dom_явный, dow_явный); dow: 0=воскресенье
_CronFields = tuple[set[int], set[int], set[int], set[int], set[int], bool, bool]


def _parse_cron(expr: str) -> _CronFields:
    parts = expr.split()
    if len(parts) != 5:
        msg = f"cron expression {expr!r}: expected 5 fields, got {len(parts)}"
        raise ValueError(msg)
    minute = _parse_field(parts[0], 0, 59, {})
    hour = _parse_field(parts[1], 0, 23, {})
    dom = _parse_field(parts[2], 1, 31, {})
    month = _parse_field(parts[3], 1, 12, _MONTH_NAMES)
    dow = {d % 7 for d in _parse_field(parts[4], 0, 7, _DOW_NAMES)}  # 7 ≡ 0 (вс)
    return minute, hour, dom, month, dow, parts[2] != "*", parts[4] != "*"


def _day_matches(fields: _CronFields, day: datetime) -> bool:
    _, _, dom, month, dow, dom_set, dow_set = fields
    if day.month not in month:
        return False
    dom_ok = day.day in dom
    dow_ok = (day.weekday() + 1) % 7 in dow  # python: понедельник=0; cron: вс=0
    if dom_set and dow_set:
        return dom_ok or dow_ok  # POSIX: оба ограничены явно → ИЛИ
    return dom_ok and dow_ok


def _cron_search(fields: _CronFields, logical_date: datetime, backwards: bool) -> datetime:
    """Ближайший момент расписания строго до/после ``logical_date``."""
    minute, hour, *_ = fields
    tod = sorted((h, m) for h in hour for m in minute)
    day = logical_date.date()
    # строгие до/после — с секундами (croniter: get_prev(23:59:59) = 23:59:00)
    t_secs = logical_date.hour * 3600 + logical_date.minute * 60 + logical_date.second
    step_days = -1 if backwards else 1
    for i in range(_MAX_DAYS):
        if _day_matches(fields, datetime.combine(day, logical_date.time())):
            if backwards:
                bound = t_secs if i == 0 else 86400
                candidates = [x for x in tod if x[0] * 3600 + x[1] * 60 < bound]
                if candidates:
                    h, m = candidates[-1]
                    # cron-гранулярность — минута: секунды логической даты не наследуем
                    return datetime.combine(day, logical_date.time()).replace(
                        hour=h, minute=m, second=0, microsecond=0
                    )
            else:
                bound = t_secs if i == 0 else -1
                candidates = [x for x in tod if x[0] * 3600 + x[1] * 60 > bound]
                if candidates:
                    h, m = candidates[0]
                    return datetime.combine(day, logical_date.time()).replace(
                        hour=h, minute=m, second=0, microsecond=0
                    )
        day += timedelta(days=step_days)
    msg = f"cron schedule: no matching time within {_MAX_DAYS} days of {logical_date}"
    raise RuntimeError(msg)


def next_tick(logical_date: datetime, schedule: object) -> datetime:
    """Первый тик расписания строго после ``logical_date`` (``data_interval_end`` Airflow).

    Только cron-строки/алиасы (как у ``prev_next``); вызывающий сам сводит
    ``None``/``@once`` к ``logical_date`` и прибавляет ``timedelta``.
    """
    if not isinstance(schedule, str):
        msg = f"schedule {schedule!r}: expected a cron string (or @alias)"
        raise ValueError(msg)
    return _cron_search(_parse_cron(_CRON_ALIASES.get(schedule, schedule)), logical_date, False)


def prev_next(logical_date: datetime, schedule: object) -> tuple[datetime | None, datetime | None]:
    """Живая семантика 2.10: ``(prev_execution_date, next_execution_date)``.

    prev — последний тик строго до ``logical_date`` (``previous_schedule``);
    next — конец текущего интервала, последний тик не позже ``logical_date``
    (на сетке — сам ``logical_date``). ``None``/``@once`` → ``(None, None)``;
    неизвестный вид расписания — громкий ``ValueError``.
    """
    if schedule is None or schedule == "@once":
        return None, None
    # конвенция шима — naive (ср. _cron_search): нормализуем вход один раз
    ld = logical_date.replace(tzinfo=None)
    if isinstance(schedule, timedelta):
        return ld - schedule, ld
    if isinstance(schedule, str):
        fields = _parse_cron(_CRON_ALIASES.get(schedule, schedule))
        prev = _cron_search(fields, ld, True)
        minute, hour, *_ = fields
        on_tick = (
            ld.second == 0
            and ld.microsecond == 0
            and (ld.hour, ld.minute) in set((h, m) for h in hour for m in minute)
            and _day_matches(fields, ld)
        )
        return prev, (ld if on_tick else prev)
    msg = (
        f"schedule {schedule!r}: the shim can only emulate None/@once/"
        f"timedelta/cron string (profile 2.10 prev/next keys)"
    )
    raise ValueError(msg)
