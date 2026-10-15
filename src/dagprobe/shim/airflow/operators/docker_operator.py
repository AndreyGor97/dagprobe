"""Shim 2.x-пути: `docker_operator` — реэкспорт."""

from dagprobe.shim.airflow.providers.docker.operators.docker import DockerOperator

__all__ = ["DockerOperator"]
