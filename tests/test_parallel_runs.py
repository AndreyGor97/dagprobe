"""D1–D2: параллельность двух прогонов одного DAG в одном процессе (гипотеза 4)."""

from __future__ import annotations

import threading

from dagprobe.shim.xcom import RunContext, xcom


def test_parallel_runs_isolated() -> None:
    """Два прогона в разных потоках не видят XCom друг друга."""
    errors: list[str] = []

    def run(run_id: str, value: str) -> None:
        try:
            RunContext.activate(run_id)
            xcom.set("extract", "return_value", value)
            barrier.wait()  # оба прогона записали — момент максимального пересечения
            got = xcom.get("extract")
            if got != value:
                errors.append(f"{run_id}: увидел чужое значение {got!r} вместо {value!r}")
        except Exception as exc:  # noqa: BLE001 — любая ошибка потока = провал теста
            errors.append(f"{run_id}: {exc}")
        finally:
            RunContext.deactivate()

    barrier = threading.Barrier(2)
    t1 = threading.Thread(target=run, args=("run_a", "A-value"))
    t2 = threading.Thread(target=run, args=("run_b", "B-value"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == []


def test_xcom_requires_run_context() -> None:
    """Вне RunContext доступ к XCom — громкая ошибка, а не тихий глобал."""
    RunContext.activate("r1")
    xcom.set("t", "return_value", 1)
    RunContext.deactivate()
    try:
        xcom.get("t")
        raise AssertionError("должен был упасть без RunContext")
    except RuntimeError:
        pass
