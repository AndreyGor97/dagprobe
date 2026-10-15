"""Shim ``providers.cncf.kubernetes.secret``: Secret — шаблонное значение для пода."""

from __future__ import annotations

from typing import Any


class Secret:
    """Стаб: deploy_type/имя храним; манифесты k8s не собираются."""

    template_fields = ("name",)

    def __init__(self, deploy_type: str, name: str | None = None, **kwargs: Any) -> None:
        self.deploy_type = deploy_type
        self.name = name
        for key, value in kwargs.items():
            setattr(self, key, value)
