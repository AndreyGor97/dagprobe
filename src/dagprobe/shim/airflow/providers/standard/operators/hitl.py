"""Shim ``providers.standard.operators.hitl``: HITLOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class HITLOperator(BaseOperator):
    """Стаб: human-in-the-loop подтверждение — рантайм Airflow."""
