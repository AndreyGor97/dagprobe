"""Дымовой тест: пакет импортируется и выставляет версию."""

import dagprobe


def test_package_metadata() -> None:
    assert dagprobe.__doc__
