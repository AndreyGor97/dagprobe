"""Файловый/pandas-стаб (§8.7, Срез 5): подмена точек входа pandas.

Правила фикстуры ``files:`` матчатся по glob пути (§8.7 ``match: path_glob``), а не по
``conn_id``: DAG хардкодит ``/data/orders.csv``, файла на диске нет — DataFrame приходит
из фикстуры. Запись в путь под правило уходит в Run Namespace (§6.23), реальная ФС не
трогается.

Поэтому же, если правила ``files:`` заданы, любой файловый вызов pandas с путём вне
правил — громкая ошибка, а не тихий поход на реальный диск (§8.6 safe-by-default):
в прогоне на фикстурах чтение боевой ФС — это уже не тест, а сюрприз.

Отличия от буквы §8.7 (сознательные):
- ``delimiter`` в фикстуре не задаётся: файл задан сырым текстом (``csv:``), разделитель
  содержится в самих байтах, угадывать нечего — парсит штатный pandas своими kwargs.
  ``encoding`` обязателен (текст→байты иначе угадывается), см. ``fixtures._load_files``.
- Fallback-уровень §8.7 (Срез 25): ``builtins.open`` и ``pathlib.Path.open`` (в 3.15
  ``read_text``/``write_text`` идут мимо ``builtins.open`` — только через ``Path.open``)
  идут по тем же правилам ``files:``: чтение отдаёт текст фикстуры, запись — в Run
  Namespace, путь вне правил — громкий ``FileNotStubbed``. ``os.remove``/``os.rename``
  не перехвачены (задачи ими не пользуются; при первом случае — дополнить здесь же).
"""

from __future__ import annotations

import fnmatch
import functools
import io
import os
import threading
from collections.abc import Callable
from typing import Any

from dagprobe.fixtures import FileRule, FixtureNotFound

_READERS = ("read_csv", "read_parquet", "read_excel", "read_json")
_WRITERS = ("to_csv", "to_json", "to_parquet", "to_excel")
_PREVIEW_LIMIT = 2000

_local = threading.local()
_saved: dict[str, Any] = {}


class FileNotStubbed(Exception):
    """Правила ``files:`` заданы, а путь им не подчиняется — реальную ФС не читаем (§8.6)."""


def install() -> None:
    """Навесить перехват на pandas и open (идемпотентно).

    Процесс прогона одноразовый (§6.23), поэтому патч процесса — не конфликт:
    на боевой ФС при этом не попадает ничего, см. ``FileNotStubbed``.
    """
    if _saved:
        return
    import builtins
    import pathlib

    import pandas as pd

    for name in _READERS:
        orig = getattr(pd, name)
        _saved[name] = orig
        setattr(pd, name, _reader(orig))
    for name in _WRITERS:
        orig = getattr(pd.DataFrame, name)
        _saved[f"DataFrame.{name}"] = orig
        setattr(pd.DataFrame, name, _writer(orig))
    # fallback §8.7: builtins.open + Path.open (read_text/write_text в 3.15
    # маршрутизируются через Path.open, мимо builtins.open)
    _saved["open"] = builtins.open
    fopen = _open(builtins.open)
    builtins.open = fopen
    _saved["Path.open"] = pathlib.Path.open
    pathlib.Path.open = lambda self, mode="r", *a, **k: fopen(self, mode, *a, **k)  # type: ignore[method-assign]


def uninstall() -> None:
    """Снять перехват (нужно только тестам: прогон живёт в своём процессе)."""
    if not _saved:
        return
    import builtins
    import pathlib

    import pandas as pd

    for name in _READERS:
        setattr(pd, name, _saved[name])
    for name in _WRITERS:
        setattr(pd.DataFrame, name, _saved[f"DataFrame.{name}"])
    builtins.open = _saved["open"]
    pathlib.Path.open = _saved["Path.open"]  # type: ignore[method-assign]
    _saved.clear()


def written() -> dict[str, str]:
    """Что задачи «записали» — Run Namespace (§6.23), на диск не уходит."""
    return dict(getattr(_local, "written", None) or {})


def reset() -> None:
    _local.written = {}


def _rules() -> tuple[FileRule, ...]:
    try:
        return tuple(_intercept().fixtures().files)
    except FixtureNotFound:
        return ()


def _intercept() -> Any:
    # локальный импорт: _intercept при активации фикстур зовёт install() отсюда
    from dagprobe.shim import _intercept as mod

    return mod


def match(path: Any) -> FileRule | None:
    """Правило для пути: паттерн с ``/`` — по всему пути, без ``/`` — по имени файла.

    ``*`` матчит и разделители (fnmatch, не pathlib-glob) — предсказуемо для
    ``/data/*.csv`` и ``*.csv``. ``\\`` нормализуется, диск ``D:`` отбрасывается:
    DAG на Windows и фикстура, написанная по-posix'овски, должны совпасть.
    """
    posix = str(path).replace("\\", "/")
    stripped = posix[2:] if len(posix) > 1 and posix[1] == ":" else posix
    name = posix.rsplit("/", 1)[-1]
    for rule in _rules():
        glob = rule.path_glob
        if "/" in glob:
            if fnmatch.fnmatch(posix, glob) or fnmatch.fnmatch(stripped, glob):
                return rule
        elif fnmatch.fnmatch(name, glob):
            return rule
    return None


def _stub(path: Any) -> FileRule:
    rules = _rules()
    rule = match(path)
    if rule is None:
        listed = ", ".join(r.path_glob for r in rules)
        msg = (
            f"path {path!s} is not covered by files: fixtures ({listed}); "
            f"the run never touches the real filesystem (§8.6) — add a rule to the fixture"
        )
        raise FileNotStubbed(msg)
    return rule


def _reader(orig: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(orig)
    def wrapper(path_or_buf: Any, *args: Any, **kwargs: Any) -> Any:
        # буфер/StringIO — не файл, пропускаем как есть (пути тут нет)
        if isinstance(path_or_buf, (str, os.PathLike)) and _rules():
            rule = _stub(path_or_buf)
            import pandas as pd

            if rule.rows is not None:
                return pd.DataFrame(rule.rows)
            # csv-текст → байты объявленной кодировкой → парсит штатный pandas
            assert rule.csv is not None and rule.encoding is not None
            return orig(io.BytesIO(rule.csv.encode(rule.encoding)), *args, **kwargs)
        return orig(path_or_buf, *args, **kwargs)

    return wrapper


def _writer(orig: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(orig)
    def wrapper(self: Any, path_or_buf: Any = None, *args: Any, **kwargs: Any) -> Any:
        if isinstance(path_or_buf, (str, os.PathLike)) and _rules():
            _stub(path_or_buf)  # правило есть — пишем в namespace, нет — громкая ошибка
            out = orig(self, None, *args, **kwargs)  # as-text/bytes, без записи на диск
            _record(str(path_or_buf), _preview(out))
            return None
        return orig(self, path_or_buf, *args, **kwargs)

    return wrapper


def _open(orig: Callable[..., Any]) -> Callable[..., Any]:
    """Fallback §8.7: open() по правилам ``files:`` — чтение из фикстуры, запись в namespace.

    ``os``-уровень (remove/rename) не перехвачен — см. docstring модуля.
    """
    write_flags = ("w", "a", "x")

    @functools.wraps(orig)
    def wrapper(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, (str, os.PathLike)) and _rules():
            rule = match(file)
            if rule is None:
                rule = _stub(file)  # громкая ошибка с перечнем правил
            binary = "b" in mode
            if any(m in mode for m in write_flags):
                buf: io.StringIO | io.BytesIO = io.BytesIO() if binary else io.StringIO()
                real_close = buf.close

                def _capture() -> None:
                    data = buf.getvalue()
                    text = data if isinstance(data, str) else f"<{len(data)} bytes>"
                    _record(str(file), text, append="a" in mode)
                    real_close()

                buf.close = _capture  # type: ignore[method-assign]
                return buf
            if rule.rows is not None:
                msg = (
                    f"path {file!s}: rows-rule has no raw text for open() (§8.7) — "
                    f"use csv: for paths read via open()"
                )
                raise FileNotStubbed(msg)
            assert rule.csv is not None and rule.encoding is not None
            data = rule.csv.encode(rule.encoding)
            return io.BytesIO(data) if binary else io.StringIO(rule.csv)
        return orig(file, mode, *args, **kwargs)

    return wrapper


def _record(path: str, text: str, append: bool = False) -> None:
    # ключ нормализуем: на Windows pathlib отдаёт \data\x.csv, на Linux /data/x.csv —
    # ответ прогона не должен зависеть от ОС
    key = path.replace("\\", "/")
    now = dict(_local.written or {})
    now[key] = now[key] + text if append and key in now else text
    _local.written = now


def _preview(out: Any) -> str:
    if isinstance(out, bytes):
        return f"<{len(out)} bytes>"
    text = str(out)
    return text if len(text) <= _PREVIEW_LIMIT else text[:_PREVIEW_LIMIT] + "…"
