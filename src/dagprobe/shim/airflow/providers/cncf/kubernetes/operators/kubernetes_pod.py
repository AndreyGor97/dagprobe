"""Shim 2.x-пути: ``operators.kubernetes_pod`` — алиас pod."""

from dagprobe.shim.airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

__all__ = ["KubernetesPodOperator"]
