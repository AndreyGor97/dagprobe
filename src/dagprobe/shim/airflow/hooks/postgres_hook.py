"""Shim 2.x-пути: ``airflow.hooks.postgres_hook`` — PostgresHook."""

from dagprobe.shim.airflow.providers.postgres.hooks.postgres import PostgresHook

__all__ = ["PostgresHook"]
