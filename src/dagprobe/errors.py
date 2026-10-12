"""Типизированная модель ошибок (RFC-04 файла 01): единая для CLI/JSON/будущих LSP.

Коды стабильны между версиями; severity — фиксированный набор. Таблица
code → exit code появляется в Срезе 6 (CI-режим), сейчас CLI выходит 1 при
любой error-жестокости.
"""

from __future__ import annotations

import dataclasses

Severity = str  # Literal без typing-литералов: значения фиксируются ниже

ERROR = "error"
WARNING = "warning"
INFO = "info"

# стабильные коды; добавлять можно, менять смысл — нельзя
IMPORT_FAILURE = "IMPORT_FAILURE"
SANDBOX_TIMEOUT = "SANDBOX_TIMEOUT"
DUPLICATE_TASK_ID = "DUPLICATE_TASK_ID"
DEPRECATED_API = "DEPRECATED_API"
NONDETERMINISTIC_TIME = "NONDETERMINISTIC_TIME"
IMPLICIT_CATCHUP = "IMPLICIT_CATCHUP"
SENSOR_NO_TIMEOUT = "SENSOR_NO_TIMEOUT"
TASK_FAILURE = "TASK_FAILURE"  # провал задачи в прогоне (Срез 6: CI-форматы)
UPSTREAM_FAILED = "UPSTREAM_FAILED"  # триггер-правило не выполнено (Срез 7)


@dataclasses.dataclass(frozen=True)
class DAGProbeError:
    """Одна находка: код, жестокость, сообщение, локация в пользовательском коде."""

    code: str
    severity: Severity
    message: str
    file: str | None = None
    line: int | None = None

    def to_dict(self) -> dict[str, object]:
        return dataclasses.asdict(self)
