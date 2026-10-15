"""Shim ``providers.cncf.kubernetes.operators.pod``: KubernetesPodOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class KubernetesPodOperator(BaseOperator):
    """Стаб: image/name и пр. — атрибуты; запуск пода — рантайм Airflow/кластера."""

    template_fields = ("image", "name", "cmds", "arguments")
