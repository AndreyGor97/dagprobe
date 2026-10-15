"""Shim ``providers.sftp.operators.sftp``: SFTPOperator."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class SFTPOperator(BaseOperator):
    """Стаб: local/remote пути — шаблонные поля; передача — рантайм Airflow."""

    template_fields = ("local_filepath", "remote_filepath")
