"""Shim ``airflow.utils.trigger_rule``: TriggerRule — str-Enum (сравним со строками)."""

from __future__ import annotations

from enum import StrEnum


class TriggerRule(StrEnum):
    """Значения — строки движка Airflow; ``TriggerRule.ALL_SUCCESS == "all_success"``."""

    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    ONE_DONE = "one_done"
    NONE_FAILED = "none_failed"
    NONE_SKIPPED = "none_skipped"
    NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"
    ALWAYS = "always"
    # 2.x-алиас, в 3.x удалён — но валидный код 2.x читаем и диагностируем:
    DUMMY = "all_success"
