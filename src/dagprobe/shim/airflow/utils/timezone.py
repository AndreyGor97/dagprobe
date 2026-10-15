"""Shim ``airflow.utils.timezone``: utcnow/utc/конвертация aware-дат."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

utc = UTC
timedelta = timedelta  # реэкспорт для «from airflow.utils.timezone import timedelta»


def utcnow() -> datetime:
    return datetime.now(tz=utc)


def make_aware(d: datetime) -> datetime:
    return d if d.tzinfo is not None else d.replace(tzinfo=utc)


def convert_to_utc(d: datetime) -> datetime:
    return make_aware(d)


def is_naive(d: datetime) -> bool:
    return d.tzinfo is None or d.tzinfo.utcoffset(d) is None


def is_localized(d: datetime) -> bool:
    return not is_naive(d)
