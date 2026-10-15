"""Shim-пакет 2.x-пути ``airflow.operators`` (реэкспорты; Срез RealDag 2.7 + 30)."""

from dagprobe.shim.airflow.operators.bash import BashOperator
from dagprobe.shim.airflow.operators.branch import BaseBranchOperator
from dagprobe.shim.airflow.operators.dummy import DummyOperator
from dagprobe.shim.airflow.operators.email import EmailOperator
from dagprobe.shim.airflow.operators.empty import EmptyOperator
from dagprobe.shim.airflow.operators.latest_only import LatestOnlyOperator
from dagprobe.shim.airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from dagprobe.shim.airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from dagprobe.shim.airflow.operators.subdag import SubDagOperator
from dagprobe.shim.airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dagprobe.shim.airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from dagprobe.shim.airflow.providers.apache.hive.operators.hive import HiveOperator
from dagprobe.shim.airflow.providers.docker.operators.docker import DockerOperator
from dagprobe.shim.airflow.providers.http.operators.http import (
    HttpOperator,
    SimpleHttpOperator,
)
from dagprobe.shim.airflow.providers.http.sensors.http import HttpSensor
from dagprobe.shim.airflow.providers.postgres.operators.postgres import PostgresOperator
from dagprobe.shim.airflow.providers.sftp.operators.sftp import SFTPOperator
from dagprobe.shim.airflow.providers.slack.operators.slack import SlackAPIPostOperator

__all__ = [
    "BashOperator",
    "BaseBranchOperator",
    "BranchPythonOperator",
    "DockerOperator",
    "DummyOperator",
    "EmailOperator",
    "EmptyOperator",
    "HiveOperator",
    "HttpOperator",
    "HttpSensor",
    "LatestOnlyOperator",
    "PostgresOperator",
    "PythonOperator",
    "S3FileTransformOperator",
    "S3KeySensor",
    "SFTPOperator",
    "ShortCircuitOperator",
    "SimpleHttpOperator",
    "SlackAPIPostOperator",
    "SubDagOperator",
    "TriggerDagRunOperator",
]
