"""Shim 2.x-пути ``airflow.operators.s3_file_transform_operator``."""

from __future__ import annotations

from dagprobe.shim._core import BaseOperator


class S3FileTransformOperator(BaseOperator):
    """Стаб: input/output ключи — шаблонные поля; трансформация — рантайм."""

    template_fields = ("input_key", "output_key")
