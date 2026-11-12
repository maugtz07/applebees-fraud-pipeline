"""
Silver: checks, item lines and voids, flattened from bronze_checks.

Every table is keyed on LocationId + BusinessDate + CheckId (the source Id is
not unique on its own). No rows are deleted: unusual cases are flagged with
Is*/Has* columns and counted by the expectations.
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def conf(key, default=None):
    value = spark.conf.get(key, default)
    if value is None:
        raise ValueError(f"Missing pipeline configuration key: {key}")
    return value


KEY = ["LocationId", "BusinessDate", "CheckId"]
KEYS_NOT_NULL = {
    "location_id_not_null": "LocationId IS NOT NULL",
    "business_date_not_null": "BusinessDate IS NOT NULL",
    "check_id_not_null": "CheckId IS NOT NULL",
}

# Spelling variants of the same void category, merged into one name.
VOID_ALIASES = json.loads(
    conf("void_category_aliases", '{"SERV ERR": "SERVER ERR", "NOT RECEVD": "NOT RECVD"}')
)


def key_columns():
    return [
        F.col("LocationId").cast("long").alias("LocationId"),
        F.expr("try_cast(BusinessDate AS DATE)").alias("BusinessDate"),
        F.col("Id").cast("long").alias("CheckId"),
    ]


def item_line_count():
    return F.when(F.col("ItemsSold").isNull(), F.lit(0)).otherwise(F.size("ItemsSold"))


@dp.materialized_view(name="silver_checks", comment="One row per check: header fields and the check owner.")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
@dp.expect("check_has_items", "HasItems")
@dp.expect("no_service_charge", "coalesce(ServiceCharge, 0) = 0")
def silver_checks():
    raw = spark.read.table("bronze_checks")
    brand = F.col("brand").cast("string") if "brand" in raw.columns else F.lit(None).cast("string")
    n_items = item_line_count()
    return raw.select(
        *key_columns(),
        brand.alias("Brand"),
        F.col("RevenueCenterId").cast("long").alias("RevenueCenterId"),
        F.expr("try_cast(OpenTime AS TIMESTAMP)").alias("OpenTime"),
        F.expr("try_cast(CloseTime AS TIMESTAMP)").alias("CloseTime"),
        "TableName",
        "TrafficCount",
        "ServiceCharge",
        F.expr("try_element_at(ItemsSold, 1).EmployeeId").cast("long").alias("CheckOwnerEmployeeId"),
        n_items.alias("ItemLineCount"),
        (n_items > 0).alias("HasItems"),
        "_source_file",
    )


ITEM_FIELDS = [
    "Sequence", "ParentSequence", "ItemId", "ItemName", "ItemPLU",
    "ItemMajorCatId", "ItemMajorCatName", "ItemSubCatId", "ItemSubCatName",
    "EmployeeId", "RevenueCenterId", "Seat", "PosSourceId",
    "Quantity", "GrossPrice", "SoldPrice",
]


@dp.materialized_view(name="silver_items_sold", comment="One row per item line. SoldPrice is the line total after item discounts.")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
@dp.expect("sold_price_not_negative", "NOT IsNegativeSoldPrice")
@dp.expect("quantity_not_zero", "NOT IsZeroQuantity")
def silver_items_sold():
    return (
        spark.read.table("bronze_checks")
        .select(*key_columns(), F.explode("ItemsSold").alias("i"))
        .select(
            *KEY,
            *[F.col(f"i.{c}").alias(c) for c in ITEM_FIELDS],
            F.expr("try_cast(i.OrderTime AS TIMESTAMP)").alias("OrderTime"),
        )
        .withColumn("IsZeroQuantity", F.coalesce(F.col("Quantity") == 0, F.lit(False)))
        .withColumn("IsNegativeSoldPrice", F.coalesce(F.col("SoldPrice") < 0, F.lit(False)))
    )


VOID_FIELDS = [
    "ItemId", "ItemName", "ItemPLU", "Quantity", "Amount",
    "EmployeeId", "ManagerId", "VoidCategoryId", "VoidCategoryName",
]


def unified_void_category(col):
    clean = F.upper(F.trim(col))
    result = clean
    for variant, canonical in VOID_ALIASES.items():
        result = F.when(clean == variant.upper(), F.lit(canonical.upper())).otherwise(result)
    return result


@dp.materialized_view(name="silver_voids", comment="One row per void. VoidCategory merges spelling variants; the original name is kept.")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
@dp.expect("void_category_present", "VoidCategory IS NOT NULL")
@dp.expect("void_not_self_approved", "NOT IsSelfApproved")
def silver_voids():
    return (
        spark.read.table("bronze_checks")
        .select(
            *key_columns(),
            (item_line_count() == 0).alias("IsCheckWithoutItems"),
            F.posexplode("Voids").alias("VoidSeq", "v"),
        )
        .select(
            *KEY,
            F.col("VoidSeq").cast("long").alias("VoidSeq"),
            *[F.col(f"v.{c}").alias(c) for c in VOID_FIELDS],
            unified_void_category(F.col("v.VoidCategoryName")).alias("VoidCategory"),
            "IsCheckWithoutItems",
        )
        .withColumn(
            "IsSelfApproved",
            F.coalesce((F.col("ManagerId") == F.col("EmployeeId")) & (F.col("ManagerId") != 0), F.lit(False)),
        )
    )
