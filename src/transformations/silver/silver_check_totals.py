"""
Silver: money totals per check and the reconciliation test.

A check reconciles when:
    payments + gratuity (negative) = item sales + tax - check-level discounts
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

KEY = ["LocationId", "BusinessDate", "CheckId"]

NUMERIC_TOTALS = [
    "GrossSales", "ItemSales", "TaxAmount", "CheckDiscountAmount", "ItemDiscountAmount",
    "DiscountCount", "SelfApprovedDiscountCount", "VoidCount", "VoidAmount",
    "PaymentAmount", "GratuityAmount", "TipAmount", "BillAmount", "CardPaymentCount", "CashPaymentCount",
]


@dp.materialized_view(name="silver_check_totals", comment="One row per check: sales, tax, discount, void, payment and tip totals.")
@dp.expect("check_reconciles", "IsReconciled")
def silver_check_totals():
    items = spark.read.table("silver_items_sold").groupBy(KEY).agg(
        F.sum("GrossPrice").alias("GrossSales"),
        F.sum("SoldPrice").alias("ItemSales"),
    )
    taxes = spark.read.table("silver_taxes").groupBy(KEY).agg(F.sum("Amount").alias("TaxAmount"))
    discounts = spark.read.table("silver_discounts").groupBy(KEY).agg(
        F.sum(F.when(F.col("Level") == "check", F.col("Amount")).otherwise(0)).alias("CheckDiscountAmount"),
        F.sum(F.when(F.col("Level") == "item", F.col("Amount")).otherwise(0)).alias("ItemDiscountAmount"),
        F.count("*").alias("DiscountCount"),
        F.sum(F.col("IsSelfApproved").cast("int")).alias("SelfApprovedDiscountCount"),
    )
    voids = spark.read.table("silver_voids").groupBy(KEY).agg(
        F.count("*").alias("VoidCount"),
        F.sum("Amount").alias("VoidAmount"),
    )
    payments = spark.read.table("silver_payments").groupBy(KEY).agg(
        F.sum("Amount").alias("PaymentAmount"),
        F.sum("Gratuity").alias("GratuityAmount"),
        F.sum("TipAmount").alias("TipAmount"),
        F.sum("BillAmount").alias("BillAmount"),
        F.sum(F.coalesce(F.col("IsCreditCard").cast("int"), F.lit(0))).alias("CardPaymentCount"),
        F.sum(F.coalesce(F.col("IsCashDebit").cast("int"), F.lit(0))).alias("CashPaymentCount"),
    )

    return (
        spark.read.table("silver_checks").select(*KEY)
        .join(items, KEY, "left")
        .join(taxes, KEY, "left")
        .join(discounts, KEY, "left")
        .join(voids, KEY, "left")
        .join(payments, KEY, "left")
        .fillna(0, NUMERIC_TOTALS)
        .withColumn(
            "ReconciliationDiff",
            F.round(
                F.col("PaymentAmount") + F.col("GratuityAmount")
                - (F.col("ItemSales") + F.col("TaxAmount") - F.col("CheckDiscountAmount")),
                2,
            ),
        )
        .withColumn("IsReconciled", F.abs("ReconciliationDiff") <= 0.01)
    )
