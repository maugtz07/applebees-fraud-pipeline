"""
Bronze: raw checks, loaded incrementally with Auto Loader.

Reads every JSON file under <source_root>/<source_checks_dir> exactly as it
arrives, nested arrays included. Each run picks up only files it has not seen
before, so new days of data are added without reprocessing old ones.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def conf(key, default=None):
    """Read a pipeline configuration value; fail with a clear message if it is missing."""
    value = spark.conf.get(key, default)
    if value is None:
        raise ValueError(f"Missing pipeline configuration key: {key}")
    return value


SOURCE_ROOT = conf("source_root").rstrip("/")
CHECKS_DIR = conf("source_checks_dir", "facts/checks").strip("/")
JSON_MULTILINE = conf("json_multiline", "true")
# Each source file holds a whole day of checks in one JSON array, so it must be
# parsed in one piece. Reading a few files per batch keeps memory under control.
MAX_FILES_PER_BATCH = conf("checks_max_files_per_batch", "1")


@dp.table(
    name="bronze_checks",
    comment="Raw checks as delivered: one row per check with nested arrays "
            "(ItemsSold, Discounts, Payments, Voids, Tax). Loaded with Auto Loader.",
)
@dp.expect("check_id_present", "Id IS NOT NULL")
@dp.expect("schema_matches_source", "_rescued_data IS NULL")
def bronze_checks():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", JSON_MULTILINE)
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_BATCH)
        .option("pathGlobFilter", "*.json")
        .load(f"{SOURCE_ROOT}/{CHECKS_DIR}")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
