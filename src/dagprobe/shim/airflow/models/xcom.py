"""Shim ``airflow.models.xcom``: BaseXCom — база кастомных XCom-бэкендов."""


class BaseXCom:
    """Стаб: значения XCom живут в dagprobe.shim.xcom, метастор не эмулируется."""
