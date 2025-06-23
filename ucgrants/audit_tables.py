import abc

from pyspark.sql import DataFrame


class AuditTables(abc.ABC):
    @abc.abstractmethod
    def read_audit_table(self, df: DataFrame, mode: str = "overwrite") -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def write_audit_table(self, df: DataFrame, mode: str = "overwrite") -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def catalog_direct_grants_table(
        self, df: DataFrame, mode: str = "overwrite"
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def schema_direct_grants_table(
        self, df: DataFrame, mode: str = "overwrite"
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def table_direct_grants_table(self, df: DataFrame) -> None:
        raise NotImplementedError


class AuditTablesUC(AuditTables):
    def __init__(self, audit_schema: str) -> None:
        self._audit_schema = audit_schema

    def read_audit_table(self, df: DataFrame, mode: str = "overwrite") -> None:
        df.write.mode(mode).saveAsTable(f"{self._audit_schema}.table_grants_read")

    def write_audit_table(self, df: DataFrame, mode: str = "overwrite") -> None:
        df.write.mode(mode).saveAsTable(f"{self._audit_schema}.table_grants_write")

    def catalog_direct_grants_table(
        self, df: DataFrame, mode: str = "overwrite"
    ) -> None:
        df.write.mode(mode).saveAsTable(f"{self._audit_schema}.catalog_direct_grants")

    def schema_direct_grants_table(
        self, df: DataFrame, mode: str = "overwrite"
    ) -> None:
        df.write.mode(mode).saveAsTable(f"{self._audit_schema}.schema_direct_grants")

    def table_direct_grants_table(self, df: DataFrame, mode: str = "overwrite") -> None:
        df.write.mode(mode).saveAsTable(f"{self._audit_schema}.table_direct_grants")
