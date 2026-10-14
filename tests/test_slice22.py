"""Срез 22: lint-правило LAZY_IMPORT — PEP 810 lazy import в DAG-файле."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from dagprobe.errors import LAZY_IMPORT, WARNING
from dagprobe.lint import lint_file

pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 15), reason="PEP 810 syntax parses only on 3.15+"
)


def _lint(tmp_path: Path, source: str) -> list[str]:
    f = tmp_path / "dag_lazy.py"
    f.write_text(source, encoding="utf-8")
    return [f"{e.code}@{e.line}" for e in lint_file(f)]


def test_lazy_import_flagged(tmp_path: Path) -> None:
    assert _lint(tmp_path, "lazy import asyncio\n") == [f"{LAZY_IMPORT}@1"]


def test_lazy_import_from_flagged(tmp_path: Path) -> None:
    assert _lint(tmp_path, "lazy from os.path import join\n") == [f"{LAZY_IMPORT}@1"]


def test_regular_import_not_flagged(tmp_path: Path) -> None:
    assert _lint(tmp_path, "import asyncio\nfrom os.path import join\n") == []


def test_lazy_import_inline_ignore(tmp_path: Path) -> None:
    assert _lint(tmp_path, "lazy import asyncio  # dagprobe: ignore=LAZY_IMPORT\n") == []


def test_severity_is_warning(tmp_path: Path) -> None:
    f = tmp_path / "dag_lazy.py"
    f.write_text("lazy import asyncio\n", encoding="utf-8")
    (err,) = lint_file(f)
    assert err.severity == WARNING
