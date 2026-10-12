# Spike — эталонные DAG и Mode A shim

## B1: эталонные DAG (источник)

Взяты из тега **`3.3.2`** официального репозитория `apache/airflow`,
путь `airflow-core/src/airflow/example_dags/` (лицензионные заголовки Apache-2.0 сохранены).

| Файл | Что покрывает (файл 03) |
|---|---|
| `tutorial.py` | Линейный DAG, `BashOperator`, `default_args`, Jinja-шаблон в `bash_command` |
| `example_branch_python_dop_operator_3.py` | Ветвление |
| `tutorial_taskflow_api.py` | TaskFlow API (`@task`) |
| `example_dynamic_task_mapping.py` | Dynamic Task Mapping (`.expand()`) |
| `example_workday_timetable.py` + `plugins/workday.py` | Кастомный `Timetable` (самый рискованный, §6.10/§6.17) |

**Отклонение от буквы файла 03:** план требовал пример с `BranchPythonOperator`.
В Airflow 3.3.2 такого примера в `example_dags` больше нет — все примеры ветвления
переписаны на декоратор `@task.branch` (TaskFlow). Взят `example_branch_python_dop_operator_3.py`
как каноничный: семантика skip/`trigger_rule` при ветвлении — та же, а именно она
и есть объект гипотезы 5.

## B2–B4: перехват импорта (Mode A)

`src/dagprobe/shim/` — meta-path finder + дерево заглушек `dagprobe.shim.airflow.*`,
зеркалящее настоящее дерево модулей Airflow. Принцип: shim-модуль добавляется только
под конкретный DAG (инкрементально, B3→B5), не «всё API сразу».
