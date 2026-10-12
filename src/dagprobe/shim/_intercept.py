"""Точка входа перехвата (§8.8 основного документа, Срезы 3–4): фикстуры в шиме.

Thread-local: фикстуры задаются на прогон (RunContext), вне прогона —
FixtureNotFound-путь ловит вызывающий. Один модуль вместо реестра классов:
пока цели перехвата — conn_id-хуки и соединение Emulated Engine.

Thread-local даёт изоляцию прогонов друг от друга (§6.23): у каждого потока
свои фикстуры и своя in-memory БД.
"""

from __future__ import annotations

import sqlite3
import threading

from dagprobe.fixtures import EmulatedConn, FixtureNotFound, Fixtures

_local = threading.local()


def set_fixtures(fixtures: Fixtures) -> None:
    _close_emulated()
    _local.fixtures = fixtures
    # файловый стаб (§8.7) ставится/снимается вместе с фикстурами: патч процесса
    # безопасен, потому что прогон живёт в своём процессе (§6.23)
    from dagprobe.shim import _fileintercept

    if fixtures.files:
        _fileintercept.reset()
        _fileintercept.install()
    else:
        _fileintercept.uninstall()


def clear_fixtures() -> None:
    _close_emulated()
    _local.fixtures = None


def fixtures() -> Fixtures:
    fx: Fixtures | None = getattr(_local, "fixtures", None)
    if fx is None:
        msg = "нет активных фикстур — прогон должен задать их явно (set_fixtures)"
        raise FixtureNotFound(msg)
    return fx


def emulated_connection(conn_id: str) -> sqlite3.Connection:
    """Соединение Emulated Engine для conn_id — одно на прогон (§8.2).

    Одно на прогон, как реальный DBAPI-хук: задача может создать временную
    таблицу и увидеть её в следующей задаче того же прогона.
    """
    cache: dict[str, sqlite3.Connection] | None = getattr(_local, "sqlite", None)
    if cache is None:
        cache = _local.sqlite = {}
    con = cache.get(conn_id)
    if con is None:
        conn = fixtures().get(conn_id)
        if not isinstance(conn, EmulatedConn):  # защита от прямого вызова мимо хука
            msg = (
                f"conn_id={conn_id!r} — это Fake-фикстура (rows); "
                f"Emulated Engine (§8.2) требует tables"
            )
            raise ValueError(msg)
        con = cache[conn_id] = conn.connect()
    return con


def _close_emulated() -> None:
    for con in (getattr(_local, "sqlite", None) or {}).values():
        con.close()
    _local.sqlite = None
