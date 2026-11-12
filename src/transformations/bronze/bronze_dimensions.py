"""
Bronze: the three small catalog files (locations, revenue centers, items),
loaded as delivered. They are small, so they are fully reloaded on every run.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def conf(key, default=None):
    value = spark.conf.get(key, default)
    if value is None:
        raise ValueError(f"Missing pipeline configuration key: {key}")
    return value


SOURCE_ROOT = conf("source_root").rstrip("/")
JSON_MULTILINE = conf("json_multiline", "true")


def read_json_folder(relative_dir):
    """Read a JSON folder. If the files turn out to be multi-line JSON, read them again that way."""
    path = f"{SOURCE_ROOT}/{relative_dir.strip('/')}"
    df = spark.read.option("multiLine", JSON_MULTILINE).json(path)
    if "_corrupt_record" in df.columns:
        df = spark.read.option("multiLine", "true").json(path)
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


@dp.materialized_view(name="bronze_locations", comment="Raw location catalog as delivered.")
@dp.expect("location_id_present", "Id IS NOT NULL")
def bronze_locations():
    return read_json_folder(conf("source_locations_dir", "dimensions/locations"))


@dp.materialized_view(name="bronze_revenue_centers", comment="Raw revenue center (sales channel) catalog as delivered.")
@dp.expect("revenue_center_id_present", "Id IS NOT NULL")
def bronze_revenue_centers():
    return read_json_folder(conf("source_revenue_centers_dir", "dimensions/revenue_centers"))


@dp.materialized_view(name="bronze_items_catalog", comment="Raw menu item catalog as delivered.")
@dp.expect("item_id_present", "Id IS NOT NULL")
def bronze_items_catalog():
    return read_json_folder(conf("source_items_dir", "dimensions/items"))
