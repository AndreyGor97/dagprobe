"""Валидация timetable-эмуляции Срез 18 против croniter (живой эталон).

croniter в dagprobe НЕ ставится (решение пользователя); он есть в venv
mode-b как зависимость Airflow 3.3.2 — им и сверяемся:

    .venv-mode-b/bin/python spike/check_timetable.py

Семантика живого 2.10 (taskinstance.py:1032-1075, зонд .scratch/probe210.py):
prev — последний тик строго до logical_date (``get_prev``); next —
data_interval_end, последний тик **не позже** logical_date
(``get_prev(logical_date + 1s)``: тики минутной гранулярности, +1s безопасен).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from croniter import croniter  # noqa: E402  (эталон, только в venv mode-b)

from dagprobe.shim import _timetable  # noqa: E402

EXPRS = [
    "@daily",
    "@hourly",
    "@weekly",
    "@monthly",
    "@yearly",
    "0 0 1 */3 *",  # = алиас @quarterly (его croniter не знает — сверяем расширение)
    "30 4 * * *",
    "*/15 * * * *",
    "0 9-17 * * 1-5",
    "5/20 3 * * *",
    "0 0 29 2 *",
    "59 23 31 12 *",
    "0,30 0,12 1,15 */2 *",
    "0 12 * * 7",
]

# выравненные по расписанию и «кривые» моменты — prev/next обязаны совпасть
GRID = [
    datetime(2026, 1, 1, tzinfo=UTC),
    datetime(2026, 1, 1, 14, 23, 7, tzinfo=UTC),
    datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC),
    datetime(2028, 2, 29, 12, 0, tzinfo=UTC),
    datetime(2026, 6, 15, 4, 29, 59, tzinfo=UTC),
]


def main() -> int:
    failures = 0
    checks = 0
    for expr in EXPRS:
        for ld in GRID:
            naive = ld.replace(tzinfo=None)
            got_prev, got_next = _timetable.prev_next(ld, expr)
            # get_prev двигает внутренний курсор — каждый раз от ld;
            # «последний тик ≤ ld» = get_prev(ld + 1s)
            it = croniter(expr, naive)
            want_prev = it.get_prev(datetime)
            it = croniter(expr, naive + timedelta(seconds=1))
            want_next = it.get_prev(datetime)
            checks += 2
            if got_prev != want_prev or (got_prev.second, got_prev.microsecond) != (0, 0):
                failures += 1
                print(f"FAIL prev {expr} @ {ld}: got {got_prev} want {want_prev}")
            if got_next != want_next or (got_next.second, got_next.microsecond) != (0, 0):
                failures += 1
                print(f"FAIL next {expr} @ {ld}: got {got_next} want {want_next}")
    # timedelta и отсутствие расписания — против croniter не сверяются:
    # семантика 2.x (арифметика / пустые строки), покрыта тестами pytest
    print(f"timetable vs croniter: {checks} checks, {failures} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
