"""Shim ``providers.http.operators.http``: HttpOperator (+ 2.x-имя SimpleHttpOperator)."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class HttpOperator(BaseOperator):
    """Стаб: endpoint/данные — шаблонные поля; вызов — рантайм Airflow."""

    template_fields = ("endpoint", "data", "headers")

    def __init__(
        self, *, http_conn_id: str | None = None, endpoint: object = None, **kwargs: object
    ) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint


SimpleHttpOperator = HttpOperator
