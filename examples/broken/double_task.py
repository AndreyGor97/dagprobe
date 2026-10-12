"""DAG с дублем `task_id` — демонстрация CI-гейта (§6.21).

`dagprobe lint --dags-folder examples/broken` отдаёт DUPLICATE_TASK_ID (error),
exit 1 и JUnit с `<failure>`: ровно то, на чём CI не пропускает мердж.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("double_task", schedule=None, start_date=None, catchup=False) as dag:
    PythonOperator(task_id="extract", python_callable=lambda **_: 1)
    PythonOperator(task_id="extract", python_callable=lambda **_: 2)
