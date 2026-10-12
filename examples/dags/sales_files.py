"""Пример Среза 5: pandas без хардкода путей (§8.7) — файловый стаб.

`/data/orders.csv` на диске нет и не должно быть: путь закрыт фикстурой
`examples/fixtures/files.yaml`, данные подставлены по glob. Запись в `/data/summary.csv`
уходит в Run Namespace (§6.23) — на реальном диске ничего не появляется, но содержимое
видно в ответе `dagprobe run` в поле `written_files`.

Запуск из клона DagProbe:

    uv run dagprobe run --dags-folder examples/dags --dag-id sales_files \\
        --fixtures examples/fixtures/files.yaml
"""

from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task

RAW = Path("/data/orders.csv")  # этого файла нет — и не нужно
OUT = Path("/data/summary.csv")  # «запись» уйдёт в память прогона

with DAG("sales_files", schedule=None, start_date=None, catchup=False) as dag:

    def extract(**_):
        orders = pd.read_csv(RAW, encoding="utf-8")
        return {"rows": len(orders), "revenue": int(orders["amount"].sum())}

    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    @task
    def load(stats):
        pd.DataFrame([stats]).to_csv(OUT, index=False)
        return f"{stats['rows']} строк, выручка {stats['revenue']}"

    load(extract_task.output)
