"""Shim Mode A: перехват ``import airflow`` без установленного Airflow (спайк, §5.3)."""

from __future__ import annotations

import sys

from dagprobe.shim.finder import DagProbeFinder

_finder: DagProbeFinder | None = None


def activate() -> None:
    """Вставить finder первым в ``sys.meta_path`` — до стандартных finder'ов."""
    global _finder
    if _finder is not None:
        return
    from dagprobe.shim import _core

    _core.reset_registry()
    _finder = DagProbeFinder()
    sys.meta_path.insert(0, _finder)


def deactivate() -> None:
    """Убрать finder и вычистить ``airflow*`` из ``sys.modules`` (изоляция тестов)."""
    global _finder
    if _finder is None:
        return
    sys.meta_path.remove(_finder)
    _finder = None
    for name in [n for n in sys.modules if n == "airflow" or n.startswith("airflow.")]:
        del sys.modules[name]
