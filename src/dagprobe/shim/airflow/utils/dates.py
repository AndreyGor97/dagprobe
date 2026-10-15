"""Shim ``airflow.utils.dates``: days_ago и разбор строк-дат."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dagprobe.shim.airflow.utils import timezone


def days_ago(
    n: int, hour: int = 0, minute: int = 0, second: int = 0, microsecond: int = 0
) -> datetime:
    """Как в Airflow: полночь (UTC) дня, наступившего ``n`` суток назад."""
    dt = timezone.utcnow() - timedelta(days=n)
    return dt.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)


def parse_execution_date(execution_date_str: str | None) -> datetime | None:
    if execution_date_str is None:
        return None

    return datetime.fromisoformat(execution_date_str.replace("Z", "+00:00")).astimezone(UTC)
