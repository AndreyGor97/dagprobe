"""Пример Среза 8: поведенческий аудит миграции Airflow 2.10 → 3.x.

``uses_execution_date`` — эталон из G2 (demand_validation_report): шаблон с
легаси-ключом контекста. В 2.10 он рендерится в дату (задача ok), в 3.x ключ
удалён из контекста (задача падает) — именно это находит ``audit``.
``uses_tomorrow_ds`` — помягче: в обеих версиях задача ok, но значение разное
(дата против пустой строки) — находка уровня ``value``. ``plain`` — чистая
задача, находок не даёт (контроль).

Запуск из клона DagProbe:

    uv run dagprobe audit 2to3 --dags-folder examples/dags --dag-id migration_legacy

Exit 1 = есть расхождения (см. JSON: ``--format json``), 0 = поведение совпадает.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def fail_on_empty(value):
    if not value:
        raise ValueError("легаси-ключ контекста не отрендерился (удалён в Airflow 3?)")
    return value


def echo(value):
    return value


with DAG("migration_legacy", schedule=None, start_date=None, catchup=False) as dag:
    PythonOperator(
        task_id="uses_execution_date",
        python_callable=fail_on_empty,
        op_kwargs={"value": "{{ execution_date }}"},
    )
    PythonOperator(
        task_id="uses_tomorrow_ds",
        python_callable=echo,
        op_kwargs={"value": "{{ tomorrow_ds }}"},
    )
    PythonOperator(
        task_id="plain",
        python_callable=echo,
        op_kwargs={"value": "{{ ds }}"},
    )
