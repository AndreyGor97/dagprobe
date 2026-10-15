"""Shim 2.x-пути: `postgres_operator` — реэкспорт."""

from dagprobe.shim.airflow.providers.postgres.operators.postgres import PostgresOperator

__all__ = ["PostgresOperator"]
