import argparse
import logging

from pyspark.sql import DataFrame, SparkSession

from ucgrants.audit_tables import AuditTablesUC
from ucgrants.grants import UCGrants

spark: SparkSession


logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


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
    parser = argparse.ArgumentParser(description="UC Grants processing tool")
    parser.add_argument("--output-catalog", required=True, help="Output catalog name")
    parser.add_argument("--output-schema", required=True, help="Output schema name")
    parser.add_argument(
        "--audit-catalog",
        action="append",
        required=True,
        help="Audit catalog names (can be specified multiple times)",
    )

    args = parser.parse_args()

    output_catalog = args.output_catalog
    output_schema = args.output_schema
    audit_schema = f"{output_catalog}.{output_schema}"
    catalog_names = args.audit_catalog

    logger.info(f"audit table schema: {output_catalog}.{output_schema}")
    logger.info(f"audit catalog names: {catalog_names}")

    get_spark_session()

    uc_grants = UCGrants.from_spark(spark=spark)
    audit_tables = AuditTablesUC(audit_schema=audit_schema)

    direct_catalog_df: DataFrame = uc_grants.direct_catalog_privileges(catalog_names)
    logger.info("writing catalog direct grants audit table")
    audit_tables.catalog_direct_grants_table(direct_catalog_df)

    direct_schema_df: DataFrame = uc_grants.direct_schema_privileges(catalog_names)
    logger.info("writing schema direct grants audit table")
    audit_tables.schema_direct_grants_table(direct_schema_df)

    direct_table_df: DataFrame = uc_grants.direct_table_privileges(catalog_names)
    logger.info("writing table direct grants audit table")
    audit_tables.table_direct_grants_table(direct_table_df)

    read_df: DataFrame = uc_grants.table_read_access(catalog_names)
    logger.info("writing reader privileges audit table")
    audit_tables.read_audit_table(read_df)

    write_df: DataFrame = uc_grants.table_write_access(catalog_names)
    logger.info("writing writer privileges audit table")
    audit_tables.write_audit_table(write_df)


if __name__ == "__main__":
    main()
