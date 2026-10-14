"""Срез 25: fallback-уровень §8.7 — ``open()``/``pathlib`` под правилами ``files:``.

Чтение по правилу отдаёт текст фикстуры, запись уходит в Run Namespace
(``written_files``), путь вне правил — громкий ``FileNotStubbed`` (§8.6):
прогон никогда не трогает реальную ФС. Прогоны — subprocess, как в Срезах 5/11.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from dagprobe.task_test import run_dag

FIXTURES_YAML = """
files:
  - match: {path_glob: "*.csv"}
    csv: |
      order_id,amount
      1,100
    encoding: utf-8
"""

RAW_OPEN = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("raw_open", schedule=None, start_date=None, catchup=False) as dag:
    def read_raw():
        with open("/data/in.csv") as fh:
            return fh.read()

    def write_raw():
        with open("/data/out.csv", "w") as fh:
            fh.write("hello")

    PythonOperator(task_id="read_raw", python_callable=read_raw)
    PythonOperator(task_id="write_raw", python_callable=write_raw)
"""

VIA_PATHLIB = (
    RAW_OPEN.replace("raw_open", "via_pathlib")
    .replace(
        """def read_raw():
        with open("/data/in.csv") as fh:
            return fh.read()""",
        """def read_raw():
        from pathlib import Path

        return Path("/data/in.csv").read_text(encoding="utf-8")""",
    )
    .replace(
        """def write_raw():
        with open("/data/out/report.txt", "w") as fh:
            fh.write("hello")""",
        """def write_raw():
        from pathlib import Path

        Path("/data/out/report.txt").write_text("hello", encoding="utf-8")""",
    )
)

UNMATCHED = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("unmatched", schedule=None, start_date=None, catchup=False) as dag:
    def sneak():
        with open("/etc/passwd") as fh:  # noqa: S108 — намеренный поход мимо правил
            return fh.read()

    PythonOperator(task_id="sneak", python_callable=sneak)
"""

NO_FIXTURES = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("no_fixtures", schedule=None, start_date=None, catchup=False) as dag:
    def real_file(path):
        with open(path) as fh:
            return fh.read()

    PythonOperator(task_id="real_file", python_callable=real_file, op_args=["__PATH__"])
"""


def _run(tmp_path: Path, name: str, source: str, fixtures_text: str | None):
    dags = tmp_path / "dags"
    dags.mkdir(exist_ok=True)
    f = dags / f"{name}.py"
    f.write_text(textwrap.dedent(source), encoding="utf-8")
    fixtures = None
    if fixtures_text is not None:
        fixtures = tmp_path / "fixtures.yaml"
        fixtures.write_text(textwrap.dedent(fixtures_text), encoding="utf-8")
    return run_dag(f, name, fixtures_path=fixtures)


def test_open_read_and_write_under_rules(tmp_path: Path) -> None:
    run = _run(tmp_path, "raw_open", RAW_OPEN, FIXTURES_YAML)
    assert run.statuses == {"read_raw": "ok", "write_raw": "ok"}
    # чтение — текст фикстуры, запись — в namespace, реальная ФС не тронута
    assert run.values["read_raw"] == repr("order_id,amount\n1,100\n")
    assert run.written["/data/out.csv"] == "hello"


def test_pathlib_routes_through_same_rules(tmp_path: Path) -> None:
    run = _run(tmp_path, "via_pathlib", VIA_PATHLIB, FIXTURES_YAML)
    assert run.statuses == {"read_raw": "ok", "write_raw": "ok"}
    assert run.values["read_raw"] == repr("order_id,amount\n1,100\n")
    assert run.written["/data/out.csv"] == "hello"


def test_open_outside_rules_is_loud(tmp_path: Path) -> None:
    run = _run(tmp_path, "unmatched", UNMATCHED, FIXTURES_YAML)
    assert run.statuses["sneak"] == "failed"
    assert "FileNotStubbed" in run.failures["sneak"]
    assert "not covered by files: fixtures" in run.failures["sneak"]


def test_open_without_file_rules_is_passthrough(tmp_path: Path) -> None:
    # правил files: нет — open() работает с реальной ФС, как до Среза 25
    real = tmp_path / "real.txt"
    real.write_text("on disk", encoding="utf-8")
    source = NO_FIXTURES.replace("__PATH__", str(real).replace("\\", "/"))
    run = _run(tmp_path, "no_fixtures", source, None)
    assert run.statuses == {"real_file": "ok"}
    assert run.values["real_file"] == repr("on disk")
