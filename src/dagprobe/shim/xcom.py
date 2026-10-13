"""D1–D2 (файл 18): параллельность — гипотеза 4 файла 03.

Две копии одного DAG в двух потоках одного процесса: XCom/значения одного прогона
не должны быть видны другому. Прообраз контекстного теста §6.23.

Airflow в реальности кладёт XCom в метабазу и изолирует по ``run_id``. Спайк-модель:
``RunContext`` (thread-local) задаёт run_id, XCom-объект читает/пишет только свой run.
"""

from __future__ import annotations

import threading
from typing import Any


class RunContext:
    """Активный прогон: thread-local ``run_id`` — изоляция параллельных прогонов."""

    _local = threading.local()

    @classmethod
    def activate(cls, run_id: str) -> None:
        cls._local.run_id = run_id

    @classmethod
    def current_run_id(cls) -> str | None:
        return getattr(cls._local, "run_id", None)

    @classmethod
    def deactivate(cls) -> None:
        cls._local.run_id = None


class XCom:
    """Хранилище значений задач, изолированное по run_id (в Airflow — метабаза)."""

    def __init__(self) -> None:
        self._store: dict[tuple[str, str, str], Any] = {}  # (run_id, task_id, key)
        self._lock = threading.Lock()

    def set(self, task_id: str, key: str, value: Any) -> None:
        run_id = self._active_run()
        with self._lock:
            self._store[(run_id, task_id, key)] = value

    def get(self, task_id: str, key: str = "return_value") -> Any:
        run_id = self._active_run()
        with self._lock:
            if (run_id, task_id, key) not in self._store:
                msg = f"XCom {task_id!r}/{key!r} not found in run {run_id!r}"
                raise KeyError(msg)
            return self._store[(run_id, task_id, key)]

    def latest(self, key: str = "return_value") -> Any:
        """Последний по записи XCom этого прогона (семантика 2.x ``xcom_pull(task_ids=None)``)."""
        run_id = self._active_run()
        with self._lock:
            vals = [v for (r, _t, k), v in self._store.items() if r == run_id and k == key]
        return vals[-1] if vals else None

    def _active_run(self) -> str:
        run_id = RunContext.current_run_id()
        if run_id is None:
            msg = "XCom accessed outside an active RunContext"
            raise RuntimeError(msg)
        return run_id


# ponytail: глобальный синглтон для спайка; в MVP — контекст на прогон (§8.8 основного).
xcom = XCom()
