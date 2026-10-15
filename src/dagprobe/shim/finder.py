"""``sys.meta_path`` finder: маршрутизирует ``import airflow`` на shim-модули dagprobe.

Гипотеза 1 файла 03 (спайк). Схема — §5.3 основного документа: finder вставляется
в ``sys.meta_path`` **перед** стандартными finder'ами. ``airflow`` и его подмодули
перенаправляются на зеркало ``dagprobe.shim.airflow.*``.

Покрытие инкрементально (файл 18, B3→B5): shim-модуль появляется только под конкретный DAG.
Того, чего в зеркале нет, finder не касается — импорт уходит к обычным finder'ам и без
установленного Airflow падает с ``ModuleNotFoundError``: явный сигнал «не покрыто shim'ом».

Механика: ``SourceFileLoader`` шима привязан к имени ``dagprobe.shim.…`` и не может
исполнить модуль под именем ``airflow.…``, поэтому отдаём собственный relay-загрузчик:
он импортирует шим-модуль как обычно и копирует его namespace в модуль-«airflow».
"""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
from collections.abc import Sequence
from types import ModuleType

_SHIM_ROOT = "dagprobe.shim."
# Корни, чьи поддеревья зеркалятся целиком: airflow + сторонние плагины (2.x-совместимость).
_SHIMMED_ROOTS = ("airflow", "airflow_clickhouse_plugin")


class _ShimRelayLoader(importlib.abc.Loader):
    """Копирует namespace шим-модуля в модуль, запрошенный под именем ``airflow.*``."""

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return None  # стандартный пустой module object

    def exec_module(self, module: ModuleType) -> None:
        shim = importlib.import_module(_SHIM_ROOT + module.__name__)
        # Копируем только публичное содержимое: dunders шима (__name__ и пр.) не должны
        # затирать служебные атрибуты модуля-«airflow».
        for key, value in shim.__dict__.items():
            if not (key.startswith("__") and key.endswith("__")):
                module.__dict__[key] = value
        # Поведенческие dunders relay'им явно: без __getattr__ не работает ленивый
        # реэкспорт корня (``from airflow import DAG`` — BACKLOG, найдено).
        for key in ("__getattr__", "__all__"):
            if hasattr(shim, key):
                module.__dict__[key] = getattr(shim, key)
        if getattr(shim, "__file__", None):
            module.__file__ = shim.__file__
        if getattr(shim, "__path__", None):
            module.__path__ = shim.__path__


class DagProbeFinder(importlib.abc.MetaPathFinder):
    """Перехватывает ``airflow``/``airflow.*``, существующие в зеркале; остальное не задевает."""

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if not any(fullname == r or fullname.startswith(r + ".") for r in _SHIMMED_ROOTS):
            return None
        try:
            spec = importlib.util.find_spec(_SHIM_ROOT + fullname)
        except (ImportError, ValueError):  # нет родителя в shim-дереве — не наше
            return None
        if spec is None:
            return None
        is_package = spec.submodule_search_locations is not None
        return importlib.util.spec_from_loader(fullname, _ShimRelayLoader(), is_package=is_package)
