# Applebee's / RMH Fraud Detection Pipeline

A Lakeflow Declarative Pipeline in Databricks that turns raw point-of-sale exports into validated tables for spotting unusual discount, tip and void patterns.

It was built on 115 days of data from 127 locations (2.1M checks, 28.6M item lines, about 10 GB of JSON). Nothing in the code is tied to that dataset: paths, schema, business rules and quality tolerances all come from the pipeline configuration, so the same code can run on another brand, another period or a larger export with the same structure.

## Repository layout

```
applebees-fraud-pipeline/
├── databricks.yml                         # bundle: variables (catalog, schema, data path)
├── resources/
│   ├── jobs/
│   │   └── fraud_detection.job.yml        # job that refreshes the pipeline
│   └── pipelines/
│       └── fraud_detection.pipeline.yml   # pipeline definition and configuration
└── src/
    └── transformations/
        ├── bronze/    # raw data as delivered
        ├── silver/    # clean, flat tables
        ├── quality/   # quality gate and exceptions
        └── gold/      # fraud-detection features
```

`resources/` says how the pipeline and job run. `src/transformations/` holds the logic for every table.

## How the pipeline works

```
raw JSON files (Volume)
      │
      ▼
bronze   bronze_checks (Auto Loader, incremental) + 3 catalog tables
      │
      ▼
silver   checks · items_sold · voids · discounts · payments · taxes
         locations · revenue_centers · items_catalog · check_totals
      │
      ▼
quality  quality_gate  ──  a hard rule fails → the update stops, gold is not built
         quality_exceptions
      │
      ▼
gold     ticket_features · emp_discount_vs_location
         emp_tip_vs_location · emp_voids_vs_location
```

Databricks works out the order from the code: each table reads the tables it needs and the pipeline graph is drawn from those reads. Gold reads `quality_gate`, so gold only refreshes when the hard quality rules pass.

**Modeling decisions**

- Every check-level table uses the key `LocationId + BusinessDate + CheckId`. The source `Id` repeats across locations and days, so it can't be used alone.
- Silver never deletes rows. Unusual records are flagged (`HasItems`, `IsNegativeSoldPrice`, `IsSelfApproved`, ...) and counted by expectations.
- The source stores tips as negative `Gratuity`. Silver adds `TipAmount = ABS(Gratuity)` and `BillAmount = Amount - TipAmount`, and keeps the original column.
- Check-level and item-level discounts live in one table with a `Level` column, so they add up without double counting.
- Bronze keeps `_source_file` and `_ingested_at` on every row, so any number can be traced back to its file.

## Data quality

Quality is enforced with pipeline expectations. Results for every run appear in the pipeline's **Data quality** tab and in its event log.

**Hard rules** (`quality_gate`, the update fails if any is broken)

| Rule | Checks that |
| --- | --- |
| `check_key_is_unique` | no two checks share the composite key |
| `no_*_lost` | silver has exactly as many checks, items, voids, discounts, payments and taxes as the arrays in bronze |
| `reconciliation_within_tolerance` | the share of checks where payments + gratuity ≠ items + tax − check discount stays under `dq_max_pct_unreconciled_checks` |
| key columns not null | enforced on every silver fact table |

**Soft rules** (logged as warnings, the run continues)

| Rule | Default tolerance |
| --- | --- |
| `self_approval_within_tolerance` | ≤ 1% of discounts |
| `checks_without_items_within_tolerance` | ≤ 2% of checks |
| `negative_prices_within_tolerance` | ≤ 0.1% of item lines |
| `no_missing_business_days` | no gaps between the first and last day |
| row-level rules on silver | manual discounts have a manager, cash has no tip, no positive gratuity, and so on |

`quality_exceptions` lists the exact records behind each rule (unreconciled checks, self-approved discounts, negative prices, ...) with their check key.

The first version of this project used fixed counts from the exploratory analysis as thresholds (for example, exactly 78 unreconciled checks). Those numbers only make sense for one dataset, so they were replaced with percentages.

## Running it

**Option A, from the bundle (pipeline and job created from the YAML files)**

1. In Databricks, create a Git folder from this repository (Workspace → Create → Git folder).
2. Open the folder, open the deployments panel and click **Deploy**. Alternatively, with the Databricks CLI: `databricks bundle deploy`.
3. Run the job `applebees_fraud_detection_job`, or the pipeline `applebees_fraud_detection` directly.

**Option B, from the UI**

1. Jobs & Pipelines → Create → ETL pipeline.
2. Source code: the `src/transformations` folder of this repository. Root folder: `src`.
3. Default catalog `workspace`, schema `fraud_ldp`, serverless compute.
4. Add the keys listed under `configuration` in `resources/pipelines/fraud_detection.pipeline.yml`.
5. Run the pipeline.

## Running on Databricks Free Edition

Each source file holds a full day of checks as a single ~90 MB line of JSON. Serverless compute on Free Edition has little memory, so three settings in `fraud_detection.pipeline.yml` keep ingestion stable:

| Setting | Why |
| --- | --- |
| `spark.databricks.photon.jsonScan.enabled: "false"` | Photon's JSON reader reserves more memory than serverless gives it; the standard Spark reader is used instead |
| `json_multiline: "true"` | parses each file as a stream instead of copying the whole 90 MB line into memory |
| `checks_max_files_per_batch: "1"` and `spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles: "3"` | Auto Loader reads one file per batch and works out the schema from three files, not all of them |

On larger compute these can be relaxed. Auto Loader keeps its progress, so if a run stops halfway, the next run continues from the last file it finished.

When the pipeline is created for the first time, a table can read an empty version of a table created in that same run. If a first run fails with `TABLE_DOES_NOT_EXIST` on a `__materialization_...` table, or a gold table comes out empty, run the pipeline once more.

## Using it with another dataset

The input has to follow the same export format: one JSON record per check with the nested arrays `ItemsSold`, `Discounts`, `Payments`, `Voids` and `Tax`, plus the location, revenue center and item catalogs. Beyond that, change only the configuration:

| Key | What it controls | Default |
| --- | --- | --- |
| `source_root` | Volume with the raw files | `/Volumes/workspace/default/applebeesdataset` |
| `source_checks_dir` | folder with the check files | `facts/checks` |
| `source_locations_dir`, `source_revenue_centers_dir`, `source_items_dir` | catalog folders | `dimensions/...` |
| `json_multiline` | `true` reads each file as one JSON document (this dataset: one array per day); `false` for one JSON record per line | `true` |
| `checks_max_files_per_batch` | how many check files Auto Loader reads per batch; raise it on larger compute | `1` |
| `automatic_discount_names` | discounts the system applies without a manager | `PUNCHHOLO,GC BONUS` |
| `void_category_aliases` | spelling variants of void categories to merge (JSON) | `{"SERV ERR": "SERVER ERR", ...}` |
| `dq_max_pct_*` | quality tolerances, in percent | see above |
| `min_checks_per_employee` | minimum checks before an employee is compared | `100` |
| `min_employees_per_location` | minimum employees for a location to be ranked | `8` |
| `tip_min_bill` | smallest bill used in tip percentages | `10` |

Point `catalog`/`schema` in `databricks.yml` at a new schema to keep each dataset's tables apart. New files dropped into the same folder are picked up on the next run without reprocessing the old ones.

## Reference results (Applebee's / RMH, Apr 28 – Aug 20, 2026)

| Measure | Value |
| --- | --- |
| Checks | 2,142,846 |
| Item lines | 28,558,925 |
| Net sales | $84,294,596 |
| Discounts (check + item) | $3,023,168 |
| Tips | $8,834,790 |
| Checks that don't reconcile | 78 (0.004%) |
| Self-approved discounts | 661 (0.13%) |

The gold tables mark employees whose behavior is far from their own location's median. They are candidates for review, not accusations.
