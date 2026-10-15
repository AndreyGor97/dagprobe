"""Shim ``providers.docker.operators.docker``: DockerOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class DockerOperator(BaseOperator):
    """Стаб: image/command — шаблонные поля; запуск контейнера — рантайм."""

    template_fields = ("image", "command")
