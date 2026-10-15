"""Shim ``airflow.utils.state``: DagRunState/TaskInstanceState — str-Enum метастора."""

from __future__ import annotations

from enum import StrEnum


class DagRunState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    @classmethod
    def is_completed(cls, state: str) -> bool:
        return state in (cls.SUCCESS, cls.FAILED)


class TaskInstanceState(StrEnum):
    REMOVED = "removed"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"  # 2.x: shutdown
    RESTARTING = "restarting"
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    DEFERRED = "deferred"
