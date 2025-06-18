import logging

from pyspark.sql import SparkSession

from ucgrants.grants import UCGrants

spark: SparkSession


logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def get_spark_session() -> None:
    global spark
    try:
        spark
    except NameError:
        try:
            from databricks.connect import DatabricksSession

            spark = DatabricksSession.builder.getOrCreate()

        except ModuleNotFoundError:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()


def main() -> None:
    get_spark_session()
    uc_grants = UCGrants.from_spark(spark=spark)

    catalog_names = ["dw_vicroads_demo"]

    # read_df = uc_grants.table_read_access()
    # read_df = uc_grants.table_read_access(catalog_names=catalog_names)
    # print(read_df.count())
    # read_df.show(n=800, truncate=False)

    cat_all = uc_grants.direct_catalog_privileges(catalog_names)
    cat_all.show(n=800, truncate=False)
    sch_all = uc_grants.direct_schema_privileges(catalog_names)
    sch_all.show(n=800, truncate=False)
    tab_all = uc_grants.direct_table_privileges(catalog_names)
    tab_all.show(n=800, truncate=False)


if __name__ == "__main__":
    main()
