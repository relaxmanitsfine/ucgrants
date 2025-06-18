import logging
from typing import Final

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from ucgrants import LOOKUP_WORKSPACE_GROUPS_DEFAULT, MAX_GROUP_MEMBERS
from ucgrants.grantees import Grantee, GranteeResolver, GranteeResolverWorkspaceClient
from ucgrants.sdk import WorkspaceClientPlus
from ucgrants.system_tables import UCSystemTables, UCSystemTablesDatabricks

logger = logging.getLogger(__name__)

# Note OWNER is not a real UC privilige but a placeholder for onwer of the object.
OWNER_PRIVILEGE: Final = "OWNER"

CATALOG_ACCESS_PRIVILEGES: Final = ("USE_CATALOG", "ALL_PRIVILEGES", OWNER_PRIVILEGE)
SCHEMA_ACCESS_PRIVILEGES: Final = ("USE_SCHEMA", "ALL_PRIVILEGES", OWNER_PRIVILEGE)
SCHEMA_FILTER: Final = ("information_schema",)
CATALOGS_FILTER: Final = ("system", "samples")

TABLE_READ_PRIVILEGES: Final = ("ALL_PRIVILEGES", OWNER_PRIVILEGE, "SELECT")
TABLE_WRITE_PRIVILEGES: Final = ("ALL_PRIVILEGES", OWNER_PRIVILEGE, "MODIFY")

GRANTEE_SCHEMA: Final = "principal_type string, principal_id string, principal string, principal_display_name string, grantee_key string"

READ_ACCESS_LABEL: Final = "read"
WRITE_ACCESS_LABEL: Final = "write"


def table_filter(privileges: set[str]) -> Column:
    sorted_privileges = sorted(set(privileges))
    conditions = [
        F.array_contains(F.col("privilege_type"), privilege)
        for privilege in sorted_privileges
    ]
    if not conditions:
        return F.lit(False)
    filter_expr = conditions[0]
    for cond in conditions[1:]:
        filter_expr = filter_expr | cond
    return filter_expr


def table_read_filter(read_privileges: set[str]) -> Column:
    return table_filter(privileges=read_privileges)


def table_write_filter(read_privileges: set[str], write_privileges: set[str]) -> Column:
    return table_filter(privileges=read_privileges) & table_filter(
        privileges=write_privileges
    )


class UCGrants:
    def __init__(
        self,
        system_tables: UCSystemTables,
        grantee_resolver: GranteeResolver,
        spark: SparkSession,
        schemas_ignore: set[str] | None = None,
        catalogs_ignore: set[str] | None = None,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> None:
        self._system_tables = system_tables
        self._grantee_resolver = grantee_resolver
        self._spark = spark
        self._lookup_workspace_groups = lookup_workspace_groups
        self._max_members = max_members
        if schemas_ignore is None:
            schemas_ignore = set(SCHEMA_FILTER)
        self._schemas_ignore = set(schemas_ignore)
        if catalogs_ignore is None:
            catalogs_ignore = set(CATALOGS_FILTER)
        self._catalogs_ignore = set(catalogs_ignore)

    @classmethod
    def from_spark(
        cls,
        spark: SparkSession,
        schemas_ignore: set[str] | None = None,
        catalogs_ignore: set[str] | None = None,
        lookup_workspace_groups: bool = LOOKUP_WORKSPACE_GROUPS_DEFAULT,
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> "UCGrants":
        system_tables = UCSystemTablesDatabricks(spark=spark)
        w = WorkspaceClientPlus()
        grantee_resolver = GranteeResolverWorkspaceClient(databricks_client=w)
        return cls(
            system_tables=system_tables,
            grantee_resolver=grantee_resolver,
            spark=spark,
            schemas_ignore=schemas_ignore,
            catalogs_ignore=catalogs_ignore,
            lookup_workspace_groups=lookup_workspace_groups,
            max_members=max_members,
        )

    def catalog_owners(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        catalogs_df: DataFrame = self._system_tables.catalogs()
        catalog_owners_df: DataFrame = catalogs_df.filter(
            F.col("catalog_name").isin(catalogs)
        ).select(
            F.col("catalog_owner").alias("grantee"),
            F.col("catalog_name"),
            F.lit(OWNER_PRIVILEGE).alias("privilege_type"),
        )
        return catalog_owners_df

    def catalog_privileges(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        catalog_privileges_df: DataFrame = self._system_tables.catalog_privileges()

        privileges_df: DataFrame = catalog_privileges_df.filter(
            F.col("catalog_name").isin(catalogs)
        ).select("grantee", "catalog_name", "privilege_type")

        return privileges_df

    def catalog_access(self, catalog_names: list[str], flat: bool = False) -> DataFrame:
        catalogs = list(catalog_names)
        catalog_owners_df = self.catalog_owners(catalogs)
        catalog_privileges_df = self.catalog_privileges(catalogs)

        catalog_all_privs_df = catalog_owners_df.unionAll(catalog_privileges_df)
        catalog_all_enriched_df = self._enrich_grantees(catalog_all_privs_df, flat=flat)

        if flat:
            return catalog_all_enriched_df
        else:
            catalog_access_df = (
                catalog_all_enriched_df.filter(
                    F.col("privilege_type").isin(list(CATALOG_ACCESS_PRIVILEGES))
                )
                .groupBy(
                    "principal",
                    "principal_id",
                    "principal_type",
                    "principal_display_name",
                    "catalog_name",
                )
                .agg(F.collect_set("privilege_type").alias("privilege_type"))
            )

            return catalog_access_df

    def schema_owners(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        schemas_df: DataFrame = self._system_tables.schemas()

        schema_owners_df: DataFrame = schemas_df.filter(
            F.col("catalog_name").isin(catalogs)
            & (~F.col("schema_name").isin(list(self._schemas_ignore)))
        ).select(
            F.col("schema_owner").alias("grantee"),
            F.col("catalog_name"),
            F.col("schema_name"),
            F.lit(OWNER_PRIVILEGE).alias("privilege_type"),
        )

        return schema_owners_df

    def schema_privileges(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        schema_privileges_df: DataFrame = self._system_tables.schema_privileges()

        privileges_df: DataFrame = schema_privileges_df.filter(
            F.col("catalog_name").isin(catalogs)
            & (~F.col("schema_name").isin(list(self._schemas_ignore)))
        ).select("grantee", "catalog_name", "schema_name", "privilege_type")

        return privileges_df

    def schema_access(self, catalog_names: list[str], flat: bool = False) -> DataFrame:
        catalogs = list(catalog_names)
        schema_owners_df = self.schema_owners(catalogs)
        schema_privileges_df = self.schema_privileges(catalogs)

        schema_all_privs_df = schema_owners_df.unionAll(schema_privileges_df)
        schema_all_enriched_df = self._enrich_grantees(schema_all_privs_df, flat=flat)

        if flat:
            return schema_all_enriched_df
        else:
            schema_access_df = (
                schema_all_enriched_df.filter(
                    F.col("privilege_type").isin(list(SCHEMA_ACCESS_PRIVILEGES))
                )
                .groupBy(
                    "principal",
                    "principal_id",
                    "principal_type",
                    "principal_display_name",
                    "catalog_name",
                    "schema_name",
                )
                .agg(F.collect_set("privilege_type").alias("privilege_type"))
            )

            return schema_access_df

    def table_owners(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        tables_df: DataFrame = self._system_tables.tables()

        table_owners_df: DataFrame = tables_df.filter(
            F.col("table_catalog").isin(catalogs)
            & (~F.col("table_schema").isin(list(self._schemas_ignore)))
        ).select(
            F.col("table_owner").alias("grantee"),
            F.col("table_catalog").alias("catalog_name"),
            F.col("table_schema").alias("schema_name"),
            F.col("table_name").alias("table_name"),
            F.lit(OWNER_PRIVILEGE).alias("privilege_type"),
        )

        return table_owners_df

    def table_privileges(self, catalog_names: list[str]) -> DataFrame:
        catalogs = [F.lit(catalog_name) for catalog_name in catalog_names]
        table_privileges_df: DataFrame = self._system_tables.table_privileges()

        privileges_df: DataFrame = table_privileges_df.filter(
            F.col("table_catalog").isin(catalogs)
            & (~F.col("table_schema").isin(list(self._schemas_ignore)))
        ).select(
            F.col("grantee"),
            F.col("table_catalog").alias("catalog_name"),
            F.col("table_schema").alias("schema_name"),
            F.col("table_name"),
            F.col("privilege_type"),
        )

        return privileges_df

    def _table_all_filtered(
        self, catalog_names: list[str], permission_filter: Column
    ) -> DataFrame:
        table_df = self._table_all(catalog_names)
        return table_df.filter(permission_filter)

    def _table_all(
        self,
        catalog_names: list[str],
        flat: bool = False,
    ) -> DataFrame:
        catalogs = list(catalog_names)
        table_owners_df = self.table_owners(catalogs)
        table_privileges_df = self.table_privileges(catalogs)

        table_all_privs_df = table_owners_df.unionAll(table_privileges_df)
        table_all_enriched_df = self._enrich_grantees(table_all_privs_df, flat=flat)

        if flat:
            return table_all_enriched_df
        else:
            table_df = table_all_enriched_df.groupBy(
                "principal",
                "principal_id",
                "principal_type",
                "principal_display_name",
                "catalog_name",
                "schema_name",
                "table_name",
            ).agg(F.collect_set("privilege_type").alias("privilege_type"))

            return table_df

    def table_read(self, catalog_names: list[str]) -> DataFrame:
        catalogs = list(catalog_names)
        permission_filer: Column = table_read_filter(set(TABLE_READ_PRIVILEGES))

        return self._table_all_filtered(catalogs, permission_filer)

    def table_write(self, catalog_names: list[str]) -> DataFrame:
        catalogs = list(catalog_names)
        permission_filter = table_write_filter(
            read_privileges=set(TABLE_READ_PRIVILEGES),
            write_privileges=set(TABLE_WRITE_PRIVILEGES),
        )

        return self._table_all_filtered(catalogs, permission_filter)

    def table_read_access(
        self, catalog_names: list[str] | None = None
    ) -> DataFrame | None:
        catalogs = self._all_catalogs(catalog_names=catalog_names)
        table_df = self.table_read(catalogs)
        return self._table_access(catalogs, table_df, READ_ACCESS_LABEL)

    def table_write_access(
        self, catalog_names: list[str] | None = None
    ) -> DataFrame | None:
        catalogs = self._all_catalogs(catalog_names=catalog_names)
        table_df = self.table_write(catalogs)
        return self._table_access(catalogs, table_df, WRITE_ACCESS_LABEL)

    def direct_catalog_privileges(
        self, catalog_names: list[str] | None = None
    ) -> DataFrame:
        catalogs = self._all_catalogs(catalog_names=catalog_names)
        catalog_df = self.catalog_access(catalogs, flat=True)
        return catalog_df

    def direct_schema_privileges(
        self, catalog_names: list[str] | None = None
    ) -> DataFrame:
        catalogs = self._all_catalogs(catalog_names=catalog_names)
        schema_df = self.schema_access(catalogs, flat=True)
        return schema_df

    def direct_table_privileges(
        self, catalog_names: list[str] | None = None
    ) -> DataFrame:
        catalogs = self._all_catalogs(catalog_names=catalog_names)
        table_df = self._table_all(catalogs, flat=True)
        return table_df

    def _all_catalogs(self, catalog_names: list[str] | None = None) -> list[str]:
        if catalog_names is None:
            catalogs_df = self._system_tables.catalogs().filter(
                ~F.col("catalog_name").isin(list(self._catalogs_ignore))
            )
            catalogs: list[str] = [
                row["catalog_name"]
                for row in catalogs_df.select("catalog_name").collect()
            ]
            return catalogs
        return list(catalog_names)

    def _table_access(
        self, catalog_names: list[str], table_df: DataFrame, table_access_label: str
    ) -> DataFrame | None:
        catalogs = list(catalog_names)
        catalog_access_df = self.catalog_access(catalogs)
        schema_access_df = self.schema_access(catalogs)

        all_access_df: DataFrame = (
            catalog_access_df.join(
                schema_access_df,
                ["principal", "principal_id", "principal_type", "catalog_name"],
                "inner",
            )
            .join(
                table_df,
                [
                    "principal",
                    "principal_id",
                    "principal_type",
                    "catalog_name",
                    "schema_name",
                ],
                "inner",
            )
            .select(
                catalog_access_df["principal"].alias("principal"),
                catalog_access_df["principal_id"].alias("principal_id"),
                catalog_access_df["principal_type"].alias("principal_type"),
                catalog_access_df["principal_display_name"].alias(
                    "principal_display_name"
                ),
                catalog_access_df["catalog_name"].alias("catalog_name"),
                schema_access_df["schema_name"].alias("schema_name"),
                table_df["table_name"].alias("table_name"),
                catalog_access_df["privilege_type"].alias("catalog_privilege"),
                schema_access_df["privilege_type"].alias("schema_privilege"),
                table_df["privilege_type"].alias("table_privilege"),
                F.lit(table_access_label).alias("table_access_label"),
            )
        )

        return all_access_df

    def _enrich_grantees(self, df: DataFrame, flat: bool = False) -> DataFrame:
        grantee_keys: set[str] = set()

        all_grantees_df: DataFrame = df.select("grantee").distinct()
        for grantee_row in all_grantees_df.collect():
            maybe_grantee_name = grantee_row.asDict(recursive=True).get("grantee")
            if maybe_grantee_name is None:
                continue
            grantee_keys.add(str(maybe_grantee_name))

        grantees: list[Grantee] = []
        for grantee_key in grantee_keys:
            if flat:
                maybe_grantee: Grantee | None = self._grantee_resolver.resolve_flat(
                    grantee_key,
                    lookup_workspace_groups=self._lookup_workspace_groups,
                    max_members=self._max_members,
                )
                if maybe_grantee is not None:
                    grantees.append(maybe_grantee)
            else:
                maybe_grantees: list[Grantee] | None = self._grantee_resolver.resolve(
                    grantee_key,
                    lookup_workspace_groups=self._lookup_workspace_groups,
                    max_members=self._max_members,
                )
                if maybe_grantees is not None:
                    grantees.extend(maybe_grantees)

        grantees_dicts: list[dict[str, str]] = [
            grantee.to_dict() for grantee in grantees
        ]
        grantees_df: DataFrame = self._spark.createDataFrame(
            grantees_dicts, schema=GRANTEE_SCHEMA
        )

        return df.join(
            grantees_df,
            df.grantee == grantees_df.grantee_key,
            how="inner",
        )
