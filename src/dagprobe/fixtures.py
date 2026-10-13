"""Fixture Manager (§8.1/§8.3): Fake-фикстуры (Срез 3) и Emulated Engine (Срез 4).

Формат §8.3 — ``conns: {conn_id: {conn_type, rows | tables}}``:

- ``rows`` — Fake: заранее заданный ответ, SQL не исполняется;
- ``tables`` — Emulated Engine (§8.2): таблицы грузятся в in-memory SQLite, и
  SQL исполняется по-настоящему (JOIN/GROUP BY/фильтры/оконные функции).

Record/Replay и Real-уровень — Срезы 7+.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

# ponytail: yaml — новая зависимость; для фикстур без вложенных спец-символов
# хватило бы json, но формат §8.3 зафиксирован YAML — берём PyYAML сразу
import yaml


@dataclasses.dataclass(frozen=True)
class FileRule:
    """Правило файлового/пайплайн-стаба (§8.7): путь по glob → данные.

    ``rows`` — готовый DataFrame (форматы без текстового представления: parquet/xlsx);
    ``csv`` — сырой текст файла, парсит штатный pandas (разделитель берётся из самих
    байтов, угадывать нечего); ``encoding`` обязателен для ``csv`` — иначе текст→байты
    угадывается, а именно этого §8.7 требует не делать.
    """

    path_glob: str
    rows: list[dict[str, Any]] | None = None
    csv: str | None = None
    encoding: str | None = None


@dataclasses.dataclass(frozen=True)
class FakeConn:
    """Заранее заданный ответ для conn_id: what get_records() вернёт."""

    conn_id: str
    conn_type: str
    rows: list[dict[str, Any]]


@dataclasses.dataclass(frozen=True)
class EmulatedConn:
    """Таблицы conn_id для Emulated Engine (§8.2) — исполняются на in-memory SQLite.

    SQLite из stdlib вместо DuckDB (§12.3 допускает оба): не тянем зависимость.
    DuckDB вернуть, когда упрёмся в его SQL-поверхность (parquet/списки).
    """

    conn_id: str
    conn_type: str
    tables: dict[str, list[dict[str, Any]]]

    def connect(self) -> sqlite3.Connection:
        """Собрать in-memory БД из фикстур: по таблице на ключ ``tables``."""
        con = sqlite3.connect(":memory:")
        for name, rows in self.tables.items():
            cols = list(dict.fromkeys(k for row in rows for k in row))
            if not cols:
                continue  # пустая таблица: заводить её нечем (колонок нет)
            quoted = ", ".join(f'"{c}"' for c in cols)
            con.execute(f'CREATE TABLE "{name}" ({quoted})')
            con.executemany(
                f'INSERT INTO "{name}" VALUES ({", ".join("?" * len(cols))})',
                [[_bind(row.get(c)) for c in cols] for row in rows],
            )
        return con


def _bind(value: Any) -> Any:
    """Привести значение из YAML к тому, что sqlite3 умеет биндить."""
    # ponytail: всё уезжает как TEXT/INT, даты — ISO-строки (сортируется верно);
    # настоящий DATE/арифметика по датам — если понадобится в тестах
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


@dataclasses.dataclass
class Fixtures:
    """Набор фикстур прогона: conn_id → FakeConn|EmulatedConn и правила ``files`` (§8.7)."""

    conns: dict[str, FakeConn | EmulatedConn] = dataclasses.field(default_factory=dict)
    files: list[FileRule] = dataclasses.field(default_factory=list)

    @classmethod
    def load(cls, path: Path) -> Fixtures:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        conns: dict[str, FakeConn | EmulatedConn] = {}
        for cid, raw in (data.get("conns") or {}).items():
            v = raw or {}
            conn_type = str(v.get("conn_type", ""))
            has_rows, has_tables = "rows" in v, "tables" in v
            if has_rows and has_tables:
                msg = (
                    f"fixture {cid!r}: set either rows (Fake) or tables "
                    f"(Emulated Engine §8.2), not both"
                )
                raise ValueError(msg)
            if has_tables:
                conns[cid] = EmulatedConn(
                    conn_id=cid,
                    conn_type=conn_type,
                    tables={str(t): list(r or []) for t, r in (v["tables"] or {}).items()},
                )
            else:
                conns[cid] = FakeConn(
                    conn_id=cid, conn_type=conn_type, rows=list(v.get("rows", []))
                )
        return cls(conns=conns, files=cls._load_files(data.get("files")))

    @staticmethod
    def _load_files(raw: Any) -> list[FileRule]:
        rules: list[FileRule] = []
        for i, item in enumerate(raw or []):
            v = item or {}
            glob = str((v.get("match") or {}).get("path_glob") or "")
            if not glob:
                msg = f"files[{i}]: match.path_glob is required (§8.7) — which path to stub"
                raise ValueError(msg)
            has_rows, has_csv = "rows" in v, "csv" in v
            if has_rows == has_csv:
                msg = (
                    f"files[{i}] ({glob}): set exactly one — rows (ready DataFrame) "
                    f"or csv (raw file text)"
                )
                raise ValueError(msg)
            encoding = v.get("encoding")
            if has_csv and not encoding:
                msg = (
                    f"files[{i}] ({glob}): csv without encoding — §8.7 requires an "
                    f"explicit encoding, otherwise data corruption is guessed silently"
                )
                raise ValueError(msg)
            rules.append(
                FileRule(
                    path_glob=glob,
                    rows=list(v["rows"]) if has_rows else None,
                    csv=str(v["csv"]) if has_csv else None,
                    encoding=str(encoding) if encoding else None,
                )
            )
        return rules

    def get(self, conn_id: str) -> FakeConn | EmulatedConn:
        try:
            return self.conns[conn_id]
        except KeyError:
            msg = f"no fixture for conn_id={conn_id!r} (FixtureNotFound)"
            raise FixtureNotFound(msg) from None


class FixtureNotFound(Exception):
    """conn_id без фикстуры — прогон не должен молча пойти в реальную систему (§8.6)."""
