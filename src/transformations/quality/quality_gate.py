"""
Quality: the gate between silver and gold.

quality_gate computes one row of metrics per run.
  - Hard rules (expect_all_or_fail): if any fails, the update stops and gold is not built.
  - Soft rules (expect_all): failures are recorded in the pipeline's Data quality tab, the run continues.

Tolerances are percentages read from the pipeline configuration, so the same
rules work for any dataset size. Results of every run are kept in the pipeline
event log; quality_exceptions lists the specific records behind each rule.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def conf(key, default=None):
    value = spark.conf.get(key, default)
    if value is None:
        raise ValueError(f"Missing pipeline configuration key: {key}")
    return value


def pct_conf(key, default):
    return float(conf(key, default))


KEY = ["LocationId", "BusinessDate", "CheckId"]

MAX_PCT_UNRECONCILED = pct_conf("dq_max_pct_unreconciled_checks", "0.01")
MAX_PCT_SELF_APPROVED = pct_conf("dq_max_pct_self_approved_discounts", "1.0")
MAX_PCT_NO_ITEMS = pct_conf("dq_max_pct_checks_without_items", "2.0")
MAX_PCT_NEGATIVE_PRICE = pct_conf("dq_max_pct_negative_sold_price", "0.1")

HARD_RULES = {
    "check_key_is_unique": "duplicate_check_keys = 0",
    "no_checks_lost": "missing_checks = 0",
    "no_items_lost": "missing_items = 0",
    "no_voids_lost": "missing_voids = 0",
    "no_discounts_lost": "missing_discounts = 0",
    "no_payments_lost": "missing_payments = 0",
    "no_taxes_lost": "missing_taxes = 0",
    "check_totals_complete": "totals_checks = silver_checks",
    "reconciliation_within_tolerance": f"pct_unreconciled_checks <= {MAX_PCT_UNRECONCILED}",
}

SOFT_RULES = {
    "no_missing_business_days": "missing_business_days = 0",
    "self_approval_within_tolerance": f"pct_self_approved_discounts <= {MAX_PCT_SELF_APPROVED}",
    "checks_without_items_within_tolerance": f"pct_checks_without_items <= {MAX_PCT_NO_ITEMS}",
    "negative_prices_within_tolerance": f"pct_negative_sold_price <= {MAX_PCT_NEGATIVE_PRICE}",
    "no_positive_gratuity": "positive_gratuity_payments = 0",
}


def pct(part, whole):
    return F.when(F.col(whole) > 0, F.round(100.0 * F.col(part) / F.col(whole), 4)).otherwise(F.lit(0.0))


@dp.materialized_view(name="quality_gate", comment="One row of data quality metrics. Gold reads this table, so gold is only built if the hard rules pass.")
@dp.expect_all_or_fail(HARD_RULES)
@dp.expect_all(SOFT_RULES)
def quality_gate():
    # What bronze says should exist (elements inside the nested arrays)
    expected = spark.read.table("bronze_checks").agg(
        F.count("*").alias("bronze_checks"),
        F.sum(F.expr("greatest(size(ItemsSold), 0)")).alias("bronze_items"),
        F.sum(F.expr("greatest(size(Voids), 0)")).alias("bronze_voids"),
        F.sum(F.expr(
            "greatest(size(Discounts), 0) + coalesce(aggregate(ItemsSold, 0, "
            "(acc, x) -> acc + greatest(size(x.Discounts), 0)), 0)"
        )).alias("bronze_discounts"),
        F.sum(F.expr("greatest(size(Payments), 0)")).alias("bronze_payments"),
        F.sum(F.expr("greatest(size(Tax), 0)")).alias("bronze_taxes"),
    )

    checks = spark.read.table("silver_checks").agg(
        F.count("*").alias("silver_checks"),
        F.countDistinct(*KEY).alias("distinct_check_keys"),
        F.sum((~F.col("HasItems")).cast("int")).alias("checks_without_items"),
        F.min("BusinessDate").alias("first_business_date"),
        F.max("BusinessDate").alias("last_business_date"),
        F.countDistinct("BusinessDate").alias("business_days"),
    )
    items = spark.read.table("silver_items_sold").agg(
        F.count("*").alias("silver_items"),
        F.sum(F.col("IsNegativeSoldPrice").cast("int")).alias("negative_sold_price_lines"),
    )
    voids = spark.read.table("silver_voids").agg(F.count("*").alias("silver_voids"))
    discounts = spark.read.table("silver_discounts").agg(
        F.count("*").alias("silver_discounts"),
        F.sum(F.col("IsSelfApproved").cast("int")).alias("self_approved_discounts"),
    )
    payments = spark.read.table("silver_payments").agg(
        F.count("*").alias("silver_payments"),
        F.sum(F.col("IsPositiveGratuity").cast("int")).alias("positive_gratuity_payments"),
    )
    taxes = spark.read.table("silver_taxes").agg(F.count("*").alias("silver_taxes"))
    totals = spark.read.table("silver_check_totals").agg(
        F.count("*").alias("totals_checks"),
        F.count(F.when(~F.col("IsReconciled"), 1)).alias("unreconciled_checks"),
    )
    metrics = (
        expected.crossJoin(checks).crossJoin(items).crossJoin(voids)
        .crossJoin(discounts).crossJoin(payments).crossJoin(taxes).crossJoin(totals)
    )

    return (
        metrics
        .withColumn("duplicate_check_keys", F.col("silver_checks") - F.col("distinct_check_keys"))
        .withColumn("missing_checks", F.abs(F.col("bronze_checks") - F.col("silver_checks")))
        .withColumn("missing_items", F.abs(F.col("bronze_items") - F.col("silver_items")))
        .withColumn("missing_voids", F.abs(F.col("bronze_voids") - F.col("silver_voids")))
        .withColumn("missing_discounts", F.abs(F.col("bronze_discounts") - F.col("silver_discounts")))
        .withColumn("missing_payments", F.abs(F.col("bronze_payments") - F.col("silver_payments")))
        .withColumn("missing_taxes", F.abs(F.col("bronze_taxes") - F.col("silver_taxes")))
        .withColumn(
            "missing_business_days",
            F.datediff("last_business_date", "first_business_date") + 1 - F.col("business_days"),
        )
        .withColumn("pct_unreconciled_checks", pct("unreconciled_checks", "silver_checks"))
        .withColumn("pct_checks_without_items", pct("checks_without_items", "silver_checks"))
        .withColumn("pct_self_approved_discounts", pct("self_approved_discounts", "silver_discounts"))
        .withColumn("pct_negative_sold_price", pct("negative_sold_price_lines", "silver_items"))
        .withColumn("checked_at", F.current_timestamp())
    )


def exception_rows(df, rule_id, detail):
    return df.select(F.lit(rule_id).alias("RuleId"), *KEY, detail.cast("string").alias("Detail"))


@dp.materialized_view(name="quality_exceptions", comment="The specific records behind each quality rule, with their check key for tracing.")
def quality_exceptions():
    totals = spark.read.table("silver_check_totals")
    discounts = spark.read.table("silver_discounts")
    payments = spark.read.table("silver_payments")
    items = spark.read.table("silver_items_sold")
    checks = spark.read.table("silver_checks")

    parts = [
        exception_rows(
            totals.filter(~F.col("IsReconciled")), "unreconciled_check",
            F.concat(F.lit("difference $"), F.col("ReconciliationDiff").cast("string")),
        ),
        exception_rows(
            discounts.filter("IsSelfApproved"), "self_approved_discount",
            F.concat_ws(" | ", F.col("DiscountName"), F.col("EmployeeId").cast("string")),
        ),
        exception_rows(
            discounts.filter("IsManagerZero AND NOT IsAutomaticDiscount"), "manual_discount_without_manager",
            F.col("DiscountName"),
        ),
        exception_rows(
            payments.filter("IsPositiveGratuity"), "positive_gratuity",
            F.concat(F.lit("Gratuity "), F.col("Gratuity").cast("string")),
        ),
        exception_rows(
            items.filter("IsNegativeSoldPrice"), "negative_sold_price",
            F.concat_ws(" | ", F.col("ItemName"), F.col("SoldPrice").cast("string")),
        ),
        exception_rows(checks.filter("NOT HasItems"), "check_without_items", F.lit("no item lines")),
    ]

    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part)
    return result
