"""Пример Среза 7: ветвление и trigger_rule в прогоне.

``route`` — BranchPythonOperator: на непустых данных выбирает ``load_events``,
иначе ``load_empty``. Невыбранные ветки получают skipped, дальше решает
trigger_rule: ``load_empty`` стоит с ``all_failed`` (не выполнено → skipped,
как в Airflow), ``report`` после ``load_events`` — обычный all_success.

``load_events`` пишет в таблицу через ``DbApiHook.insert_rows`` (Срез 7),
``report`` читает её обратно — запись видна следующей задаче прогона.

Запуск из клона DagProbe:

    uv run dagprobe run --dags-folder examples/dags --dag-id branch_etl \\
        --fixtures examples/fixtures/branch.yaml
"""

from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG
from airflow.sdk.bases.hook import DbApiHook

with DAG("branch_etl", schedule=None, start_date=None, catchup=False) as dag:

    def extract(**_):
        return DbApiHook(conn_id="dwh").get_records("SELECT * FROM events")

    def route(rows, **_):
        return "load_events" if rows else "load_empty"

    def load_events(rows, **_):
        # таблицу создаём сами: фикстура даёт только events, INSERT в несуществующую — ошибка
        DbApiHook(conn_id="dwh").run("CREATE TABLE IF NOT EXISTS events_loaded (event, n)")
        DbApiHook(conn_id="dwh").insert_rows("events_loaded", rows, target_fields=["event", "n"])
        return f"loaded {len(rows)} rows"

    def report(msg, **_):
        count = DbApiHook(conn_id="dwh").get_records("SELECT COUNT(*) AS c FROM events_loaded")
        return f"{msg}; в таблице events_loaded строк: {count[0]['c']}"

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    route_task = BranchPythonOperator(
        task_id="route", python_callable=route, op_kwargs={"rows": extract_task.output}
    )
    load_empty = PythonOperator(
        task_id="load_empty", python_callable=lambda **_: "пустые данные", trigger_rule="all_failed"
    )
    load_events_task = PythonOperator(
        task_id="load_events", python_callable=load_events, op_kwargs={"rows": extract_task.output}
    )
    report_task = PythonOperator(
        task_id="report", python_callable=report, op_kwargs={"msg": load_events_task.output}
    )

    extract_task >> route_task >> [load_events_task, load_empty]
    load_events_task >> report_task
