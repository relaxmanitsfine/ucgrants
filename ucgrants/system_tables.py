import abc

from pyspark.sql import DataFrame, SparkSession


class UCSystemTables(abc.ABC):
    @abc.abstractmethod
    def catalogs(self) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def catalog_privileges(self) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def schemas(self) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def schema_privileges(self) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def tables(self) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def table_privileges(self) -> DataFrame:
        raise NotImplementedError


class UCSystemTablesDatabricks(UCSystemTables):
    def __init__(self, spark: SparkSession) -> None:
        self._spark_session = spark

    def catalogs(self) -> DataFrame:
        catalogs_df: DataFrame = self._spark_session.table(
            "system.information_schema.catalogs"
        )
        return catalogs_df

    def catalog_privileges(self) -> DataFrame:
        catalog_privileges_df: DataFrame = self._spark_session.table(
            "system.information_schema.catalog_privileges"
        )
        return catalog_privileges_df

    def schemas(self) -> DataFrame:
        schemas_df: DataFrame = self._spark_session.table(
            "system.information_schema.schemata"
        )
        return schemas_df

    def schema_privileges(self) -> DataFrame:
        schema_privileges_df: DataFrame = self._spark_session.table(
            "system.information_schema.schema_privileges"
        )
        return schema_privileges_df

    def tables(self) -> DataFrame:
        tables_df: DataFrame = self._spark_session.table(
            "system.information_schema.tables"
        )
        return tables_df

    def table_privileges(self) -> DataFrame:
        table_privileges_df: DataFrame = self._spark_session.table(
            "system.information_schema.table_privileges"
        )
        return table_privileges_df
