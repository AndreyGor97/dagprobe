"""Shim ``airflow.configuration``: conf с дефолтами, без реального конфига."""

from __future__ import annotations

import os
from typing import Any

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "~/airflow")


class _Conf:
    """``conf.get(section, key, ...)`` всегда отдаёт default/fallback: это parse-time."""

    def get(self, section: str, key: str, default: Any = None, **kwargs: Any) -> Any:
        return kwargs.get("fallback", default)

    def getboolean(self, section: str, key: str, default: bool = False, **kwargs: Any) -> Any:
        return kwargs.get("fallback", default)

    def getint(self, section: str, key: str, default: int = 0, **kwargs: Any) -> Any:
        return kwargs.get("fallback", default)

    def getfloat(self, section: str, key: str, default: float = 0.0, **kwargs: Any) -> Any:
        return kwargs.get("fallback", default)

    def has_section(self, section: str) -> bool:
        return False

    def has_option(self, section: str, key: str) -> bool:
        return False


conf = _Conf()

# модульный алиас (корпус RealDag2: ``airflow.configuration.get(...)``)
get = conf.get


def get_custom_secret_backend() -> None:
    """Секретных бэкендов в шиме нет (§8.6 safe-by-default)."""
    return None
