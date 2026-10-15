"""Shim стороннего ``airflow_clickhouse_plugin`` (GitHub @NtwaliB, v1.x).

Parse-time: имена операторов/хуков/сенсоров и хранение полей. В прогоне
(task_test) ClickHouseHook маршрутизируется в Fixture Manager (Срез 28):
Fake — готовые rows, Emulated — SQL на SQLite; ClickHouseOperator и
ClickHouseSqlSensor.poke исполняются движком прогона.
"""
