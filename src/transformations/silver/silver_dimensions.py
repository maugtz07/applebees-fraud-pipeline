"""
Silver: clean catalogs (locations, revenue centers, menu items) with
standard column names and flags for what is actually used in the data.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="silver_locations", comment="One row per location, with IsClosed and HasSalesInPeriod flags.")
@dp.expect_or_fail("location_id_not_null", "LocationId IS NOT NULL")
def silver_locations():
    with_sales = (
        spark.read.table("silver_checks")
        .select("LocationId").distinct()
        .withColumn("HasSalesInPeriod", F.lit(True))
    )
    return (
        spark.read.table("bronze_locations")
        .select(
            F.col("Id").cast("long").alias("LocationId"),
            F.col("Name").alias("LocationName"),
            F.expr("try_cast(OpenDate AS DATE)").alias("OpenDate"),
            F.expr("try_cast(ClosedDate AS DATE)").alias("ClosedDate"),
            "TimeZoneName",
        )
        .withColumn("IsClosed", F.col("ClosedDate").isNotNull())
        .join(with_sales, "LocationId", "left")
        .withColumn("HasSalesInPeriod", F.coalesce("HasSalesInPeriod", F.lit(False)))
    )


@dp.materialized_view(name="silver_revenue_centers", comment="One row per revenue center (sales channel), with IsUsedInPeriod.")
@dp.expect_or_fail("revenue_center_id_not_null", "RevenueCenterId IS NOT NULL")
def silver_revenue_centers():
    used = (
        spark.read.table("silver_checks")
        .select("RevenueCenterId").distinct()
        .withColumn("IsUsedInPeriod", F.lit(True))
    )
    return (
        spark.read.table("bronze_revenue_centers")
        .select(
            F.col("Id").cast("long").alias("RevenueCenterId"),
            F.col("Name").alias("RevenueCenterName"),
        )
        .join(used, "RevenueCenterId", "left")
        .withColumn("IsUsedInPeriod", F.coalesce("IsUsedInPeriod", F.lit(False)))
    )


@dp.materialized_view(name="silver_items_catalog", comment="One row per menu item.")
@dp.expect_or_fail("item_id_not_null", "ItemId IS NOT NULL")
def silver_items_catalog():
    raw = spark.read.table("bronze_items_catalog")
    optional = [c for c in ["Plu", "SalesCategoryId", "ExternalId"] if c in raw.columns]
    renamed = [F.col(c).alias("ItemPLU" if c == "Plu" else c) for c in optional]
    return raw.select(
        F.col("Id").cast("long").alias("ItemId"),
        F.col("Name").alias("ItemName"),
        *renamed,
    )
