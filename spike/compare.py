"""Сравнение снимков Mode A и Mode B (Часть C). exit 1 при любом расхождении.

Снимки записываются инспектором (inspect_dags.py); перед JSON Mode B может
печатать служебный лог Airflow — срезаем с первого «{».
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent / "out"


def load(name: str) -> dict:
    text = (OUT / name).read_text()
    return json.loads(text[text.index("{") :])  # noqa: S108


def main() -> None:
    a = load("mode_a.json")
    b = load("mode_b.json")
    diffs: list[str] = []
    for dag_id in sorted(set(a) | set(b)):
        da, db = a.get(dag_id), b.get(dag_id)
        if da is None or db is None:
            diffs.append(f"[{dag_id}] есть только в {'A' if db is None else 'B'}")
            continue
        for field in ("tasks", "edges", "task_kinds", "rendered"):
            if json.dumps(da[field], sort_keys=True) != json.dumps(db[field], sort_keys=True):
                diffs.append(f"[{dag_id}] {field}: A={da[field]!r} B={db[field]!r}")
    if diffs:
        print("\n".join(diffs))
        sys.exit(1)
    print(f"OK: {len(a)} DAG идентичны в обоих режимах")


if __name__ == "__main__":
    main()
