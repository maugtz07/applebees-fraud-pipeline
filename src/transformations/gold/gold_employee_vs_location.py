"""
Gold: each employee compared with the median of their own location,
for discounts, tips (card payments only) and voids.

A high or low value marks a candidate for review, not an accusation.
IsRankable is true only when the location has enough comparable employees.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


def conf(key, default=None):
    value = spark.conf.get(key, default)
    if value is None:
        raise ValueError(f"Missing pipeline configuration key: {key}")
    return value


KEY = ["LocationId", "BusinessDate", "CheckId"]
MIN_CHECKS = int(conf("min_checks_per_employee", "100"))
MIN_EMPLOYEES = int(conf("min_employees_per_location", "8"))
TIP_MIN_BILL = float(conf("tip_min_bill", "10"))


def after_quality_gate(df):
    gate = spark.read.table("quality_gate").select(F.lit(1).alias("_quality_gate_passed"))
    return df.crossJoin(gate).drop("_quality_gate_passed")


def compare_with_location(df, metric):
    """Add the location median of `metric`, the employee's distance from it, and IsRankable."""
    medians = df.groupBy("LocationId").agg(
        F.percentile_approx(metric, 0.5).alias("LocationMedian"),
        F.count("*").alias("EmployeesInLocation"),
    )
    locations = spark.read.table("silver_locations").select("LocationId", "LocationName")
    return (
        df.join(medians, "LocationId")
        .withColumn("DiffVsLocation", F.round(F.col(metric) - F.col("LocationMedian"), 1))
        .withColumn("IsRankable", F.col("EmployeesInLocation") >= MIN_EMPLOYEES)
        .join(locations, "LocationId", "left")
    )


def owned_checks():
    return spark.read.table("gold_ticket_features").filter("HasItems")


@dp.materialized_view(name="gold_emp_discount_vs_location", comment="Discount behavior per employee vs. their location. Discounts are attributed to the check owner.")
def gold_emp_discount_vs_location():
    per_employee = (
        owned_checks()
        .groupBy("LocationId", F.col("CheckOwnerEmployeeId").alias("EmployeeId"))
        .agg(
            F.count("*").alias("Checks"),
            F.sum(F.col("HasDiscount").cast("int")).alias("ChecksWithDiscount"),
            F.sum("DiscountCount").alias("DiscountUses"),
            F.sum("SelfApprovedDiscountCount").alias("SelfApprovedUses"),
            F.sum("DiscountAmount").alias("DiscountAmount"),
            F.sum("GrossSales").alias("GrossSales"),
        )
        .filter(F.col("Checks") >= MIN_CHECKS)
        .withColumn("PctChecksWithDiscount", F.round(100 * F.col("ChecksWithDiscount") / F.col("Checks"), 1))
        .withColumn(
            "PctDiscountOfGross",
            F.when(F.col("GrossSales") > 0, F.round(100 * F.col("DiscountAmount") / F.col("GrossSales"), 2)),
        )
        .withColumn(
            "PctSelfApproved",
            F.when(F.col("DiscountUses") > 0, F.round(100 * F.col("SelfApprovedUses") / F.col("DiscountUses"), 1))
            .otherwise(F.lit(0.0)),
        )
    )

    owners = spark.read.table("silver_checks").select(*KEY, F.col("CheckOwnerEmployeeId").alias("EmployeeId"))
    ranked = Window.partitionBy("LocationId", "EmployeeId").orderBy(F.desc("count"), "DiscountName")
    top_discount = (
        spark.read.table("silver_discounts").select(*KEY, "DiscountName")
        .join(owners, KEY)
        .groupBy("LocationId", "EmployeeId", "DiscountName").count()
        .withColumn("rank", F.row_number().over(ranked))
        .filter("rank = 1")
        .select(
            "LocationId", "EmployeeId",
            F.col("DiscountName").alias("TopDiscountName"),
            F.col("count").alias("TopDiscountUses"),
        )
    )

    result = (
        compare_with_location(per_employee, "PctChecksWithDiscount")
        .join(top_discount, ["LocationId", "EmployeeId"], "left")
        .withColumn(
            "PctTopDiscount",
            F.when(F.col("DiscountUses") > 0, F.round(100 * F.col("TopDiscountUses") / F.col("DiscountUses"), 1)),
        )
    )
    return after_quality_gate(result)


@dp.materialized_view(name="gold_emp_tip_vs_location", comment="Tip behavior per employee vs. their location. Card payments only, bills above the configured minimum.")
def gold_emp_tip_vs_location():
    card = (
        spark.read.table("silver_payments")
        .filter(F.coalesce(F.col("IsCreditCard").cast("boolean"), F.lit(False)))
        .filter(F.col("BillAmount") >= TIP_MIN_BILL)
        .withColumn("TipPct", 100 * F.col("TipAmount") / F.col("BillAmount"))
        .withColumn("HasTip", F.col("TipAmount") > 0)
    )
    per_employee = (
        card.groupBy("LocationId", "EmployeeId")
        .agg(
            F.count("*").alias("CardPayments"),
            F.sum(F.col("HasTip").cast("int")).alias("PaymentsWithTip"),
            F.percentile_approx(F.when(F.col("HasTip"), F.col("TipPct")), 0.5).alias("MedianTipPct"),
            F.sum(F.when(F.col("HasTip") & (F.round("TipPct") == 10), 1).otherwise(0)).alias("TipsNear10Pct"),
        )
        .filter(F.col("CardPayments") >= MIN_CHECKS)
        .withColumn("PctWithTip", F.round(100 * F.col("PaymentsWithTip") / F.col("CardPayments"), 1))
        .withColumn("MedianTipPct", F.round("MedianTipPct", 1))
        .withColumn(
            "PctTipsNear10",
            F.when(F.col("PaymentsWithTip") > 0, F.round(100 * F.col("TipsNear10Pct") / F.col("PaymentsWithTip"), 1)),
        )
    )
    return after_quality_gate(compare_with_location(per_employee, "PctWithTip"))


@dp.materialized_view(name="gold_emp_voids_vs_location", comment="Void behavior per employee vs. their location: voids per 100 checks owned.")
def gold_emp_voids_vs_location():
    voids = spark.read.table("silver_voids").groupBy("LocationId", "EmployeeId").agg(
        F.count("*").alias("VoidCount"),
        F.sum("Amount").alias("VoidAmount"),
        F.sum(F.col("IsSelfApproved").cast("int")).alias("SelfApprovedVoids"),
        F.sum(F.col("IsCheckWithoutItems").cast("int")).alias("VoidsOnChecksWithoutItems"),
    )
    owned = (
        owned_checks()
        .groupBy("LocationId", F.col("CheckOwnerEmployeeId").alias("EmployeeId"))
        .agg(F.count("*").alias("Checks"), F.sum("ItemSales").alias("ItemSales"))
        .filter(F.col("Checks") >= MIN_CHECKS)
    )
    per_employee = (
        owned.join(voids, ["LocationId", "EmployeeId"], "left")
        .fillna(0, ["VoidCount", "VoidAmount", "SelfApprovedVoids", "VoidsOnChecksWithoutItems"])
        .withColumn("VoidsPer100Checks", F.round(100 * F.col("VoidCount") / F.col("Checks"), 1))
        .withColumn(
            "VoidPctOfSales",
            F.when(F.col("ItemSales") > 0, F.round(100 * F.col("VoidAmount") / F.col("ItemSales"), 2)),
        )
        .withColumn(
            "PctSelfApproved",
            F.when(F.col("VoidCount") > 0, F.round(100 * F.col("SelfApprovedVoids") / F.col("VoidCount"), 1))
            .otherwise(F.lit(0.0)),
        )
    )
    return after_quality_gate(compare_with_location(per_employee, "VoidsPer100Checks"))
