"""Пример Среза 9: аудит xcom_pull-семантики 2.10 → 3.x (G2 #1–#2).

Самый сильный сигнал спроса из G2 (apache/airflow#51821): после апгрейда сотни
DAG-ов молча сломались, потому что ``xcom_pull(task_ids=None)`` в 3.x берёт
XCom только текущей задачи (типично — None), а не всего dag-run, как в 2.x.
Поправка Среза 17 (13.10, живой A/B против 3.3.2): списковый ``task_ids``,
включая один элемент, возвращает список в ОБОИХ версиях — #49540 живым
эталоном не подтвердился, находки тут нет (контроль одинаковости).

Запуск из клона DagProbe:

    uv run dagprobe audit 2to3 --dags-folder examples/dags --dag-id migration_xcom
"""

from airflow.providers.standard.operators.python import (
    PythonOperator,
    get_current_context,
)
from airflow.sdk import DAG


def pull_none():
    ti = get_current_context()["ti"]
    v = ti.xcom_pull(task_ids=None)
    if v is None:
        raise ValueError(
            "xcom_pull(task_ids=None) вернул None — семантика 3.x (в 2.x был весь dag-run)"
        )
    return v


def pull_single_list():
    ti = get_current_context()["ti"]
    r = ti.xcom_pull(task_ids=["pusher"])
    return "list" if isinstance(r, list) else "value"  # обе версии: список (A/B 13.10)


def pull_string():
    ti = get_current_context()["ti"]
    return ti.xcom_pull(task_ids="pusher")  # одинаково в обеих версиях (контроль)


with DAG("migration_xcom", schedule=None, start_date=None, catchup=False) as dag:
    pusher = PythonOperator(task_id="pusher", python_callable=lambda: "v")
    taker_none = PythonOperator(task_id="taker_none", python_callable=pull_none)
    taker_list = PythonOperator(task_id="taker_list", python_callable=pull_single_list)
    taker_str = PythonOperator(task_id="taker_str", python_callable=pull_string)

    pusher >> [taker_none, taker_list, taker_str]
