"""Shim 2.x-пути ``airflow.hooks`` — хуки, которые просил корпус RealDag2."""

from dagprobe.shim.airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dagprobe.shim.airflow.providers.http.hooks.http import HttpHook
from dagprobe.shim.airflow.providers.postgres.hooks.postgres import PostgresHook
from dagprobe.shim.airflow.providers.ssh.hooks.ssh import SSHHook
from dagprobe.shim.airflow.sdk.bases.hook import BaseHook

__all__ = ["BaseHook", "HttpHook", "PostgresHook", "S3Hook", "SSHHook"]
