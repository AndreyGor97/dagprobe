"""Shim ``providers.amazon.aws.hooks.eks``: состояния кластера как имена."""

from __future__ import annotations

from enum import StrEnum


class ClusterStates(StrEnum):
    """Статусы кластера EKS (как в провайдере)."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    FAILED = "FAILED"
    UPDATING = "UPDATING"


class NodegroupStates(StrEnum):
    """Статусы nodegroup EKS."""

    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    DELETING = "DELETING"
    FAILED = "FAILED"
    UPDATING = "UPDATING"


__all__ = ["ClusterStates", "NodegroupStates"]
