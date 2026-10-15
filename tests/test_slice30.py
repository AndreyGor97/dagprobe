"""Срез 30: провайдер-стабы и дыры ядра из корпуса RealDag2.

Провайдеры (cncf/apache/amazon/slack/google/http/postgres/opensearch/ssh/jira/
sftp/docker) — классы на parse-time; ядро: exceptions, datasets, hooks (2.x),
settings/configuration, DagBag/Connection/BaseXCom, sdk.definitions.*, легаси
алиасы операторов/сенсоров 2.x, проводка ``list >> op`` и ``op >> group``.

Acceptance: мини-даг со всеми новыми импортами парсится без ошибок; края
групповой проводки совпадают с семантикой Airflow (корни/листья группы).
"""

from __future__ import annotations

from pathlib import Path

from dagprobe.analyze import analyze_file

MINI = """
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.configuration import AIRFLOW_HOME, conf, get as conf_get
from airflow.datasets import Dataset
from airflow.decorators import task_group
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.hooks import S3Hook, SSHHook
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection, DagBag, DagRun
from airflow.models.baseoperator import BaseOperator as MOBaseOperator
from airflow.models.xcom import BaseXCom
from airflow.operators import EmailOperator, HttpSensor, SimpleHttpOperator, S3KeySensor
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensor
from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, Param, get_current_context
from airflow.sdk.definitions.asset import Asset as DefinitionsAsset
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context

with DAG(
    dag_id="s30_classic",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    params={"x": Param(1)},
    on_failure_callback=send_slack_notification(text="boom"),
) as d:
    secret = Secret("env", name="my-secret")
    pod = KubernetesPodOperator(
        task_id="pod", image="python:3.12", secrets=[secret],
        retries=2, retry_delay=timedelta(minutes=1),
    )
    bq = BigQueryDeleteTableOperator(task_id="bq", deletion_dataset_table="p.d.t")
    hive = HiveOperator(task_id="hive", hql="SELECT 1")
    named = NamedHivePartitionSensor(task_id="named", partition_names=["ds=2026-10-01"])
    http = SimpleHttpOperator(task_id="http", endpoint="/api")
    check = HttpSensor(task_id="check", endpoint="/health")
    s3key = S3KeySensor(task_id="s3key", bucket_key="a/b", wildcard_match=True)
    email = EmailOperator(task_id="email", to="a@b.c", subject="s")
    fs = FileSensor(task_id="fs", filepath="/tmp/x")
    dt = DateTimeSensor(task_id="dt", target_time="2026-10-02")
    td = TimeDeltaSensor(task_id="td", delta=timedelta(hours=1))
    latest = LatestOnlyOperator(task_id="latest")
    dummy = DummyOperator(task_id="dummy")
    sub = SubDagOperator(task_id="sub", subdag=None)
    pg = PostgresHook(postgres_conn_id="pg")
    s3 = S3Hook(aws_conn_id="aws")
    ssh = SSHHook(ssh_conn_id="ssh")
    jira = JiraHook(jira_conn_id="jira")
    osh = OpenSearchHook(opensearch_conn_id="os")
    aws = AwsBaseHook(aws_conn_id="aws2")
    hh = HttpHook(method="POST")
    pgh = PostgresHook("pg2")
    bag = DagBag(dag_folder="/nope")
    ctx: Context
    ds = Dataset("s3://bucket/k")
    asset = Asset("gs://bucket/k")
    run = DagRun()
    conf.get("core", "dags_folder")
    conf_get("core", "dags_folder")
    ctx2 = get_current_context  # вызов — рантайм; импорт — parse-time
    xcom_base = BaseXCom
    hitl = HITLOperator(task_id="hitl")
    docker = DockerOperator(task_id="docker", image="img", command="run")
    sftp = SFTPOperator(task_id="sftp", local_filepath="/l", remote_filepath="/r")
    slack_post = SlackAPIPostOperator(task_id="slack_post", channel="#c", text="t")
    notifier = SlackWebhookNotifier(text="hi")
    branch_base = BaseBranchOperator

    pod >> bq >> [hive, named] >> http
    check << s3key << email << fs
    latest >> dt >> td >> dummy >> sub


@task_group
def grp30() -> None:
    pass
"""


def _write(tmp_path: Path, name: str, text: str) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    file = dags / f"{name}.py"
    file.write_text(text, encoding="utf-8")
    return file


def test_all_new_imports_parse_clean(tmp_path: Path) -> None:
    result = analyze_file(_write(tmp_path, "mini30", MINI))
    assert result.errors == [], [e.message for e in result.errors]
    classic = next(d for d in result.dags if d.dag_id == "s30_classic")
    assert "pod" in set(classic.tasks)


WIRING = """
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="wiring", start_date=datetime(2026, 10, 1), schedule=None):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    with TaskGroup("g") as g:
        g1 = EmptyOperator(task_id="g1")
        g2 = EmptyOperator(task_id="g2")
        g3 = EmptyOperator(task_id="g3")
        g1 >> [g2, g3]

    # 1) список >> задача (валится без __rrshift__)
    [a, b] >> end
    # 1b) явный API: set_upstream
    end.set_upstream(b)
    end.set_downstream(a)
    # 2) задача >> группа: в корни группы
    start >> g
    # 3) список >> группа
    [a, b] >> g
    # 4) группа >> задача: из листьев группы
    g >> end


def edges(dag) -> list[tuple[str, str]]:
    return sorted((u, t.task_id) for t in dag.task_dict.values() for u in t.upstream_task_ids)
"""


def test_list_wiring_and_group_semantics(tmp_path: Path) -> None:
    result = analyze_file(_write(tmp_path, "wiring", WIRING))
    assert result.errors == [], [e.message for e in result.errors]
    dag = next(d for d in result.dags if d.dag_id == "wiring")
    edges = set(map(tuple, dag.edges))
    # корень группы — только g1 (g2/g3 висят на нём); листья — g2 и g3
    assert {
        ("a", "end"),
        ("b", "end"),
        ("start", "g.g1"),  # start >> g: в корень
        ("a", "g.g1"),  # [a, b] >> g
        ("g.g2", "end"),  # g >> end: из листьев
        ("g.g3", "end"),
    } <= edges
    assert ("start", "g.g2") not in edges  # не в некорневые
