"""Shim ``providers.standard.sensors.base``: BaseSensorOperator (структура графа).

Покет-логика — рантайм Airflow; прогон DagProbe исполняет только сенсоры,
для которых есть fixtures-маршрутизация (ClickHouseSqlSensor, Срез 28).
"""

from __future__ import annotations

from typing import Any

from dagprobe.shim._core import BaseOperator


class BaseSensorOperator(BaseOperator):
    """Сенсор: ``poke(context) -> bool``; режим/интервалы храним как атрибуты."""

    template_fields: tuple[str, ...] = ("poke_interval", "timeout")

    def __init__(
        self,
        *,
        poke_interval: float = 60,
        timeout: float = 60 * 60 * 24,
        mode: str = "poke",
        soft_fail: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.mode = mode
        self.soft_fail = soft_fail

    def poke(self, context: dict[str, Any]) -> bool:
        msg = f"poke() of {type(self).__name__} not implemented in shim"
        raise NotImplementedError(msg)


class PokeReturnValue:
    """Стаб ответа покета: ``is_done`` + xcom-значение (исполняется рантаймом)."""

    def __init__(self, is_done: bool = False, xcom_value: Any = None) -> None:
        self.is_done = is_done
        self.xcom_value = xcom_value
