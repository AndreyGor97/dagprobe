"""Пример Среза 4: честный SQL (JOIN + GROUP BY) без Postgres и без Docker.

Данные для соединения ``dwh`` лежат в ``examples/fixtures/dwh.yaml`` (уровень
Emulated Engine §8.2): таблицы грузятся в in-memory SQLite, и запрос исполняется
по-настоящему — считает движок, а не фикстура.

Запуск из клона DagProbe:

    uv run dagprobe run --dags-folder examples/dags --dag-id db_join_etl \\
        --fixtures examples/fixtures/dwh.yaml
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task
from airflow.sdk.bases.hook import DbApiHook

with DAG("db_join_etl", schedule=None, start_date=None, catchup=False) as dag:

    def extract(**_):
        return DbApiHook(conn_id="dwh").get_records(
            "SELECT c.name AS customer, SUM(o.amount) AS revenue"
            " FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
            " WHERE o.amount > 0"
            " GROUP BY c.name ORDER BY revenue DESC"
        )

    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    @task
    def load(rows):
        assert rows, "нечего грузить: extract вернул пусто"
        return [f"{r['customer']}: {r['revenue']}" for r in rows]

    load(extract_task.output)
