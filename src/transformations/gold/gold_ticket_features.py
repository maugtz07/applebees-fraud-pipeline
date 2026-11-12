"""
Gold: one row per check with every total needed for fraud analysis.

after_quality_gate() joins the one-row quality_gate table. That join makes
gold depend on the gate: if a hard quality rule fails, gold is not refreshed.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

KEY = ["LocationId", "BusinessDate", "CheckId"]


def after_quality_gate(df):
    gate = spark.read.table("quality_gate").select(F.lit(1).alias("_quality_gate_passed"))
    return df.crossJoin(gate).drop("_quality_gate_passed")


@dp.materialized_view(name="gold_ticket_features", comment="One row per check: owner, channel, time, sales, discounts, voids, payments and tip.")
def gold_ticket_features():
    header = spark.read.table("silver_checks").select(
        *KEY, "Brand", "RevenueCenterId", "CheckOwnerEmployeeId", "HasItems", "TrafficCount", "OpenTime", "CloseTime",
    )
    totals = spark.read.table("silver_check_totals")

    features = (
        header.join(totals, KEY, "left")
        .withColumn("DiscountAmount", F.col("CheckDiscountAmount") + F.col("ItemDiscountAmount"))
        .withColumn("NetSales", F.col("ItemSales") - F.col("CheckDiscountAmount"))
        .withColumn("HasDiscount", F.col("DiscountCount") > 0)
        .withColumn("HasVoid", F.col("VoidCount") > 0)
        .withColumn("DayOfWeek", F.date_format("BusinessDate", "E"))
        .withColumn("OpenHour", F.hour("OpenTime"))
    )
    return after_quality_gate(features)
