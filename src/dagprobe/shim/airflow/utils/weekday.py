"""Shim ``airflow.utils.weekday``: WeekDay — Enum для WeekdayTimetable."""

from __future__ import annotations

from enum import Enum


class WeekDay(Enum):
    MONDAY = "MONDAY"
    TUESDAY = "TUESDAY"
    WEDNESDAY = "WEDNESDAY"
    THURSDAY = "THURSDAY"
    FRIDAY = "FRIDAY"
    SATURDAY = "SATURDAY"
    SUNDAY = "SUNDAY"

    @property
    def full_name(self) -> str:
        return self.value.capitalize()

    @classmethod
    def from_string(cls, name: str) -> WeekDay:
        try:
            return cls[name.upper()]
        except KeyError:
            msg = f"Invalid WeekDay: {name!r}"
            raise ValueError(msg) from None
