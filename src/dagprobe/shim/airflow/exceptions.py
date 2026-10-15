"""Shim ``airflow.exceptions``: имена исключений — parse-time; бросает движок Airflow."""

from __future__ import annotations


class AirflowException(Exception):
    """Базовое исключение Airflow."""


class AirflowConfigException(AirflowException):
    """Ошибка конфигурации."""


class AirflowNotFoundException(AirflowException):
    """Объект не найден (404-класс)."""


class AirflowFailException(AirflowException):
    """Задача падает без ретраев."""


class AirflowSkipException(AirflowException):
    """Задача пропускается."""


class AirflowRescheduleException(AirflowException):
    """Задача переносится на позже."""


class AirflowSensorTimeout(AirflowException):
    """Сенсор не дождался покета."""


class AirflowTaskTimeout(AirflowException):
    """Задача превысила execution_timeout."""


class AirflowTaskTerminated(AirflowException):
    """Задача снята внешне (OOM, kill)."""
