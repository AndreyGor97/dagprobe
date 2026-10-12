"""Срез 5: файловый/pandas-стаб (§8.7) — путь матчится по glob, а не по conn_id.

Проверяем ценностное предложение среза: DAG хардкодит `/data/orders.csv`, которого на
диске нет, — данные приходят из фикстуры; запись уходит в Run Namespace, а не на ФС.
"""

from __future__ import annotations

import textwrap
from collections.abc import Callable
from pathlib import Path

import pytest

from dagprobe.fixtures import Fixtures
from dagprobe.shim import _fileintercept, _intercept
from dagprobe.task_test import TaskRunResult, run_dag

READ_DAG = """
from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("read_csv", schedule=None, start_date=None, catchup=False) as dag:
    def top(**_):
        df = pd.read_csv(Path("/data/orders.csv"))
        return int(df["amount"].sum())

    PythonOperator(task_id="top", python_callable=top)
"""

WRITE_DAG = """
from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

OUT = "/data/out.csv"

with DAG("write_csv", schedule=None, start_date=None, catchup=False) as dag:
    def save(**_):
        pd.DataFrame([{"total": 105}]).to_csv(OUT, index=False)
        return Path(OUT).exists()

    PythonOperator(task_id="save", python_callable=save)
"""

WINDOWS_DAG = r"""
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG("win_path", schedule=None, start_date=None, catchup=False) as dag:
    def rows(**_):
        df = pd.read_csv(r"D:\data\orders.csv")
        df.to_csv(r"D:\data\out.csv", index=False)
        return len(df)

    PythonOperator(task_id="rows", python_callable=rows)
"""

FIXTURES_YAML = """
files:
  - match: {path_glob: "*.csv"}
    csv: |
      order_id,amount
      1,100
      2,5
    encoding: utf-8
"""

PANDAS_DF_DAG = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.bases.hook import DbApiHook

with DAG("pandas_df", schedule=None, start_date=None, catchup=False) as dag:
    def total(**_):
        df = DbApiHook(conn_id="dwh").get_pandas_df("SELECT amount FROM orders")
        return int(df["amount"].sum())

    PythonOperator(task_id="total", python_callable=total)
"""

DB_FIXTURES_YAML = """
conns:
  dwh:
    conn_type: postgres
    tables:
      orders:
        - {amount: 100}
        - {amount: 5}
        - {amount: 7}
"""


def _dag(tmp_path: Path, name: str, source: str) -> Path:
    f = tmp_path / f"{name}.py"
    f.write_text(textwrap.dedent(source), encoding="utf-8")
    return f


def _fixtures(tmp_path: Path, text: str) -> Path:
    f = tmp_path / "fixtures.yaml"
    f.write_text(textwrap.dedent(text), encoding="utf-8")
    return f


def _run(tmp_path: Path, name: str, source: str, fixtures_text: str) -> TaskRunResult:
    return run_dag(
        _dag(tmp_path, name, source), name, fixtures_path=_fixtures(tmp_path, fixtures_text)
    )


@pytest.fixture()
def in_process_stub(tmp_path: Path) -> Callable[[str], None]:
    """Стаб в этом процессе (не subprocess) — там, где нужен сам pandas."""

    def apply(text: str) -> None:
        _intercept.set_fixtures(Fixtures.load(_fixtures(tmp_path, text)))

    yield apply
    _intercept.clear_fixtures()
    _fileintercept.uninstall()


def test_read_csv_hardcoded_path_comes_from_fixture(tmp_path: Path) -> None:
    """§8.7: файла на диске нет — DataFrame всё равно приходит из фикстуры."""
    assert not Path("/data/orders.csv").exists()
    result = _run(tmp_path, "read_csv", READ_DAG, FIXTURES_YAML)
    assert result.failures == {}
    assert result.values["top"] == "105"  # 100 + 5 из текста фикстуры


def test_write_is_captured_in_namespace_not_on_disk(tmp_path: Path) -> None:
    """§8.7: запись перехвачена в Run Namespace, реальная ФС не тронута."""
    result = _run(tmp_path, "write_csv", WRITE_DAG, FIXTURES_YAML)
    assert result.failures == {}
    assert result.values["save"] == "False"  # Path.exists() на реальной ФС — false
    assert not Path("/data/out.csv").exists()
    assert result.written == {"/data/out.csv": "total\n105\n"}


def test_path_outside_rules_is_loud(tmp_path: Path) -> None:
    """Правила есть, путь не подходит → громкая ошибка, а не чтение боевой ФС (§8.6)."""
    fixture = FIXTURES_YAML.replace("*.csv", "*.parquet")
    result = _run(tmp_path, "read_csv", READ_DAG, fixture)
    assert result.statuses["top"] == "failed"
    assert "не подчиняется фикстурам" in result.failures["top"]


def test_windows_backslash_path_matches_full_pattern(tmp_path: Path) -> None:
    """Путь с `\\` (DAG на Windows) матчится по паттерну с `/`; ключ в written_files — тоже."""
    fixture = 'files:\n  - match: {path_glob: "/data/*.csv"}\n    rows: [{a: 1}]\n'
    result = _run(tmp_path, "win_path", WINDOWS_DAG, fixture)
    assert result.failures == {}
    assert result.values["rows"] == "1"
    assert result.written == {"D:/data/out.csv": "a\n1\n"}


def test_rows_rule_serves_parquet_without_engine(in_process_stub: Callable[[str], None]) -> None:
    """`rows` — готовый DataFrame: parquet читается без pyarrow (его тут и нет)."""
    in_process_stub("files:\n  - match: {path_glob: '*.parquet'}\n    rows: [{a: 1}, {a: 2}]\n")
    import pandas as pd

    assert pd.read_parquet("/data/x.parquet")["a"].tolist() == [1, 2]


def test_encoding_is_declared_and_honoured(
    in_process_stub: Callable[[str], None],
) -> None:
    """§8.7: кодировка не угадывается — объявлена в фикстуре и обязана совпасть с кодом."""
    in_process_stub(
        'files:\n  - match: {path_glob: "*.csv"}\n'
        '    csv: "city,revenue\\nМосква,10\\n"\n    encoding: cp1251\n'
    )
    import pandas as pd

    assert pd.read_csv("/data/x.csv", encoding="cp1251")["revenue"].tolist() == [10]
    with pytest.raises(UnicodeDecodeError):
        pd.read_csv("/data/x.csv")  # код не объявил кодировку — файл честно не читается


def test_csv_without_encoding_is_loud(tmp_path: Path) -> None:
    """Фикстура без encoding — ошибка загрузки, а не молчаливое угадывание (§8.7)."""
    fixture = _fixtures(tmp_path, 'files:\n  - match: {path_glob: "*.csv"}\n    csv: "a\\n1\\n"\n')
    with pytest.raises(ValueError, match="кодировк"):
        Fixtures.load(fixture)


def test_get_pandas_df_on_emulated_engine(tmp_path: Path) -> None:
    """Долг Срезов 3/4 закрыт: `get_pandas_df` работает на Emulated Engine (живой SQL)."""
    result = _run(tmp_path, "pandas_df", PANDAS_DF_DAG, DB_FIXTURES_YAML)
    assert result.failures == {}
    assert result.values["total"] == "112"
