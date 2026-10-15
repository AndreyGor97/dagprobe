"""Shim 2.x-пути: ``airflow.hooks.http_hook`` — HttpHook."""

from dagprobe.shim.airflow.providers.http.hooks.http import HttpHook

__all__ = ["HttpHook"]
