"""
Silver: discounts (check level + item level), payments and taxes.

Discounts from both levels go into one table without double counting.
Tips are stored as negative Gratuity in the source: TipAmount = ABS(Gratuity),
and BillAmount = Amount - TipAmount (the bill before the tip).
"""
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

# Discounts applied by the system (loyalty, gift cards) that legitimately have no manager.
AUTOMATIC_DISCOUNTS = [
    name.strip() for name in conf("automatic_discount_names", "PUNCHHOLO,GC BONUS").split(",") if name.strip()
]
_quoted = ", ".join("'" + name.replace("'", "''") + "'" for name in AUTOMATIC_DISCOUNTS)
MANAGER_RULE = f"NOT IsManagerZero OR DiscountName IN ({_quoted})" if AUTOMATIC_DISCOUNTS else "NOT IsManagerZero"


def source():
    return spark.read.table("bronze_checks").select(
        F.col("LocationId").cast("long").alias("LocationId"),
        F.expr("try_cast(BusinessDate AS DATE)").alias("BusinessDate"),
        F.col("Id").cast("long").alias("CheckId"),
        "Discounts", "ItemsSold", "Payments", "Tax",
    )


def discount_fields():
    return [
        F.col("d.DiscountId").alias("DiscountId"),
        F.col("d.DiscountName").alias("DiscountName"),
        F.col("d.Amount").alias("Amount"),
        F.col("d.EmployeeId").alias("EmployeeId"),
        F.col("d.ManagerId").alias("ManagerId"),
    ]


@dp.materialized_view(name="silver_discounts", comment="One row per discount use, check level and item level together.")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
@dp.expect("manual_discount_has_manager", MANAGER_RULE)
@dp.expect("discount_not_self_approved", "NOT IsSelfApproved")
def silver_discounts():
    src = source()

    check_level = (
        src.select(*KEY, F.posexplode("Discounts").alias("DiscountSeq", "d"))
        .select(
            *KEY,
            F.lit("check").alias("Level"),
            F.col("DiscountSeq").cast("long").alias("DiscountSeq"),
            F.lit(None).cast("long").alias("ItemSequence"),
            F.lit(None).cast("long").alias("ItemId"),
            *discount_fields(),
        )
    )

    item_level = (
        src.select(*KEY, F.explode("ItemsSold").alias("it"))
        .select(
            *KEY,
            F.col("it.Sequence").cast("long").alias("ItemSequence"),
            F.col("it.ItemId").cast("long").alias("ItemId"),
            F.posexplode("it.Discounts").alias("DiscountSeq", "d"),
        )
        .select(
            *KEY,
            F.lit("item").alias("Level"),
            F.col("DiscountSeq").cast("long").alias("DiscountSeq"),
            "ItemSequence",
            "ItemId",
            *discount_fields(),
        )
    )

    return (
        check_level.unionByName(item_level)
        .withColumn("IsManagerZero", F.coalesce(F.col("ManagerId") == 0, F.lit(False)))
        .withColumn(
            "IsSelfApproved",
            F.coalesce((F.col("ManagerId") == F.col("EmployeeId")) & (F.col("ManagerId") != 0), F.lit(False)),
        )
        .withColumn(
            "IsAutomaticDiscount",
            F.coalesce(F.col("DiscountName").isin(AUTOMATIC_DISCOUNTS), F.lit(False)),
        )
        .withColumn("IsMixedCaseName", F.coalesce(F.col("DiscountName") != F.upper("DiscountName"), F.lit(False)))
    )


PAYMENT_FIELDS = [
    "EmployeeId", "Amount", "Gratuity", "PayCatName", "PayCatNo", "PaymentCategoryId", "PosSourceId",
    "IsCashDebit", "IsCreditCard", "IsGiftCard", "IsGiftCert", "IsNonPOS", "IsPettyCash",
]


@dp.materialized_view(name="silver_payments", comment="One row per payment, with TipAmount (positive) and BillAmount (before tip).")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
@dp.expect("gratuity_not_positive", "NOT IsPositiveGratuity")
@dp.expect("cash_has_no_tip", "NOT (coalesce(IsCashDebit, false) AND TipAmount > 0)")
def silver_payments():
    return (
        source()
        .select(*KEY, F.posexplode("Payments").alias("PaymentSeq", "p"))
        .select(
            *KEY,
            F.col("PaymentSeq").cast("long").alias("PaymentSeq"),
            *[F.col(f"p.{c}").alias(c) for c in PAYMENT_FIELDS],
        )
        .withColumn("TipAmount", F.abs(F.coalesce(F.col("Gratuity"), F.lit(0))))
        .withColumn("BillAmount", F.col("Amount") - F.col("TipAmount"))
        .withColumn("IsPositiveGratuity", F.coalesce(F.col("Gratuity") > 0, F.lit(False)))
        .withColumn("IsNegativeAmount", F.coalesce(F.col("Amount") < 0, F.lit(False)))
    )


@dp.materialized_view(name="silver_taxes", comment="One row per tax line of a check.")
@dp.expect_all_or_fail(KEYS_NOT_NULL)
def silver_taxes():
    return (
        source()
        .select(*KEY, F.posexplode("Tax").alias("TaxSeq", "t"))
        .select(
            *KEY,
            F.col("TaxSeq").cast("long").alias("TaxSeq"),
            F.col("t.TaxCategoryId").alias("TaxCategoryId"),
            F.col("t.TaxCategoryName").alias("TaxCategoryName"),
            F.col("t.Amount").alias("Amount"),
        )
    )
