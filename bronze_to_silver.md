#!/usr/bin/env python
# coding: utf-8

# ## bronze_to_silver — Enhanced Medallion Pipeline
#
# **Purpose**: Ingest raw Parquet files from the Bronze layer, apply data quality
# checks and transformations, persist cleaned data to the Silver layer, and
# aggregate a Customer-360 view in the Gold layer.
#
# **Medallion Stages**
# | Stage  | Storage Path                         | Tables                                                      |
# |--------|--------------------------------------|-------------------------------------------------------------|
# | Bronze | Files/bronze/*.parquet               | customers, orders, payments, support, web                   |
# | Silver | Tables/silver_*                      | silver_customers, silver_orders, silver_payments, silver_support, silver_web |
# | Gold   | Tables/gold_*                        | gold_customer360                                            |
#
# **Data Lineage**:
# ```
# Parquet (Bronze files)
#   → bronze Delta tables (raw, unmodified)
#     → silver_* Delta tables (cleaned & validated)
#       → gold_customer360 (aggregated, BI-ready)
# ```
#
# **SLA / Expected Volumes** (adjust to your environment):
# | Table       | Expected rows | Max acceptable nulls (key cols) | Max processing time |
# |-------------|---------------|----------------------------------|---------------------|
# | customers   | ~50 k         | 0                                | 5 min               |
# | orders      | ~500 k        | 0                                | 10 min              |
# | payments    | ~500 k        | 0                                | 10 min              |
# | support     | ~100 k        | 0                                | 5 min               |
# | web         | ~2 M          | 0                                | 15 min              |

# ---
# ## Cell 1 — Imports & Configuration
# ---

# In[1]:


import logging
import time
import functools
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lower, trim, initcap, when, regexp_replace,
    to_date, lit, current_timestamp, count, sum as spark_sum,
    countDistinct, max as spark_max, min as spark_min,
    expr, coalesce,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, DateType, TimestampType,
)

# ---------------------------------------------------------------------------
# Logging — write to the Fabric notebook console and to a Delta audit table.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bronze_to_silver")

# ---------------------------------------------------------------------------
# Pipeline Configuration
# ---------------------------------------------------------------------------
# All tuneable parameters are centralised here so the pipeline can be driven
# by a config file or notebook widgets without touching business logic.
# ---------------------------------------------------------------------------
CONFIG: Dict = {
    # ── Storage ──────────────────────────────────────────────────────────────
    "storage_account": "ecommerce",
    "lakehouse":        "ecommerce_lakehouse.Lakehouse",
    "bronze_path":      "abfss://ecommerce@onelake.dfs.fabric.microsoft.com"
                        "/ecommerce_lakehouse.Lakehouse/Files/bronze",

    # ── Source files ─────────────────────────────────────────────────────────
    "sources": {
        "customers": "customers.parquet",
        "orders":    "orders.parquet",
        "payments":  "payments.parquet",
        "support":   "support_tickets.parquet",
        "web":       "web_activities.parquet",
    },

    # ── Incremental load ─────────────────────────────────────────────────────
    # Set to True to enable CDC / incremental mode.  When enabled, only rows
    # whose primary-key does not already exist in the Silver table are merged.
    "incremental_mode": False,

    # ── Data-quality thresholds ───────────────────────────────────────────────
    "dq_max_null_pct":       5.0,   # alert if null % on key cols exceeds this
    "dq_max_duplicate_pct":  1.0,   # alert if duplicate % exceeds this
    "dq_min_row_count":      1,     # alert if output table has fewer rows

    # ── Audit ────────────────────────────────────────────────────────────────
    "audit_table": "pipeline_audit_log",

    # ── Notification stubs ───────────────────────────────────────────────────
    # Replace the lambda with a real Teams/email/PagerDuty call in production.
    "notify_fn": lambda msg: logger.warning("🔔 NOTIFICATION: %s", msg),
}


# ---------------------------------------------------------------------------
# Explicit Schemas
# ---------------------------------------------------------------------------
# Defining schemas upfront avoids costly schema-inference scans and makes
# data-type expectations explicit for downstream consumers.
# ---------------------------------------------------------------------------
SCHEMAS: Dict[str, StructType] = {
    "customers": StructType([
        StructField("customer_id",  StringType(),  nullable=False),
        StructField("name",         StringType(),  nullable=True),
        StructField("EMAIL",        StringType(),  nullable=True),
        StructField("gender",       StringType(),  nullable=True),
        StructField("dob",          StringType(),  nullable=True),
        StructField("location",     StringType(),  nullable=True),
    ]),
    "orders": StructType([
        StructField("order_id",     StringType(),  nullable=False),
        StructField("customer_id",  StringType(),  nullable=False),
        StructField("order_date",   StringType(),  nullable=True),
        StructField("amount",       DoubleType(),  nullable=True),
        StructField("status",       StringType(),  nullable=True),
    ]),
    "payments": StructType([
        StructField("payment_id",     StringType(), nullable=False),
        StructField("customer_id",    StringType(), nullable=False),
        StructField("order_id",       StringType(), nullable=True),
        StructField("payment_date",   StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("payment_status", StringType(), nullable=True),
        StructField("amount",         DoubleType(), nullable=True),
    ]),
    "support": StructType([
        StructField("ticket_id",         StringType(), nullable=False),
        StructField("customer_id",       StringType(), nullable=False),
        StructField("ticket_date",       StringType(), nullable=True),
        StructField("issue_type",        StringType(), nullable=True),
        StructField("resolution_status", StringType(), nullable=True),
    ]),
    "web": StructType([
        StructField("session_id",   StringType(), nullable=False),
        StructField("customer_id",  StringType(), nullable=False),
        StructField("session_time", StringType(), nullable=True),
        StructField("page_viewed",  StringType(), nullable=True),
        StructField("device_type",  StringType(), nullable=True),
    ]),
}


# ---
# ## Cell 2 — Decorators & Utility Functions
# ---

# In[2]:


def timed(fn):
    """Decorator: log execution time of any pipeline function."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        logger.info("▶ Starting  : %s", fn.__qualname__)
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info("✔ Completed : %s  (%.2f s)", fn.__qualname__, elapsed)
            return result
        except Exception as exc:
            elapsed = time.perf_counter() - start
            logger.error("✘ Failed    : %s  (%.2f s) — %s", fn.__qualname__, elapsed, exc)
            raise
    return wrapper


def log_step(step: str):
    """Decorator factory: log a named pipeline step."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            logger.info("── %s ──", step)
            return fn(*args, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Data-Quality Helpers
# ---------------------------------------------------------------------------

def profile_dataframe(df: DataFrame, name: str) -> Dict:
    """
    Compute basic data-quality metrics for a DataFrame.

    Metrics returned:
        - row_count        : total rows
        - column_count     : number of columns
        - null_counts      : {col_name: null_row_count}
        - null_pct         : {col_name: null_percentage}
        - distinct_counts  : {col_name: distinct_value_count}
        - completeness_pct : average non-null percentage across all columns

    Args:
        df:   The Spark DataFrame to profile.
        name: Logical name used in log messages.

    Returns:
        Dictionary containing all computed metrics.
    """
    row_count = df.count()
    col_count = len(df.columns)

    # Compute null counts for every column in a single pass
    null_exprs = [
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    null_row = df.select(null_exprs).collect()[0].asDict()
    null_pct = {
        c: round(null_row[c] / row_count * 100, 2) if row_count > 0 else 0.0
        for c in df.columns
    }

    # Distinct counts — sampled for large tables to avoid expensive full-scan per column.
    # Tables with >500 k rows are sampled at 10%; smaller tables are fully scanned.
    _SAMPLE_THRESHOLD = 500_000
    _SAMPLE_FRACTION  = 0.10
    df_for_distinct = df.sample(fraction=_SAMPLE_FRACTION) if row_count > _SAMPLE_THRESHOLD else df
    distinct_counts = {c: df_for_distinct.select(c).distinct().count() for c in df.columns}

    completeness_pct = round(
        sum(100.0 - pct for pct in null_pct.values()) / col_count, 2
    ) if col_count > 0 else 0.0

    profile = {
        "table":           name,
        "row_count":       row_count,
        "column_count":    col_count,
        "null_counts":     null_row,
        "null_pct":        null_pct,
        "distinct_counts": distinct_counts,
        "completeness_pct": completeness_pct,
    }
    logger.info(
        "📊 Profile [%s]: rows=%d  cols=%d  completeness=%.1f%%",
        name, row_count, col_count, completeness_pct,
    )
    return profile


def assert_no_critical_nulls(
    df: DataFrame,
    key_cols: List[str],
    table_name: str,
    max_null_pct: float = 0.0,
) -> None:
    """
    Assert that key columns do not exceed the allowed null percentage.

    Args:
        df:           DataFrame to check.
        key_cols:     Columns that must not be null (or within threshold).
        table_name:   Used in log / error messages.
        max_null_pct: Allowed null percentage (default 0 = zero nulls).

    Raises:
        ValueError: If any key column exceeds the null threshold.
    """
    row_count = df.count()
    if row_count == 0:
        logger.warning("⚠ [%s] DataFrame is empty — skipping null assertion.", table_name)
        return

    for c in key_cols:
        null_count = df.filter(col(c).isNull()).count()
        pct = null_count / row_count * 100
        if pct > max_null_pct:
            msg = (
                f"[{table_name}] Column '{c}' has {null_count} nulls "
                f"({pct:.2f}% > threshold {max_null_pct}%)"
            )
            logger.error("✘ DQ failure: %s", msg)
            CONFIG["notify_fn"](f"DQ FAILURE — {msg}")
            raise ValueError(msg)
        logger.info("✔ DQ [%s.%s]: %d nulls (%.2f%%)", table_name, c, null_count, pct)


def assert_no_excessive_duplicates(
    df: DataFrame,
    id_col: str,
    table_name: str,
    max_dup_pct: float = 0.0,
) -> None:
    """
    Assert that the number of duplicate primary-key values is within threshold.

    Args:
        df:          DataFrame to check.
        id_col:      Primary-key column name.
        table_name:  Used in log messages.
        max_dup_pct: Maximum allowed percentage of duplicate rows.

    Raises:
        ValueError: If duplicates exceed the allowed threshold.
    """
    total     = df.count()
    distinct  = df.select(id_col).distinct().count()
    dup_count = total - distinct
    dup_pct   = dup_count / total * 100 if total > 0 else 0.0

    if dup_pct > max_dup_pct:
        msg = (
            f"[{table_name}] Column '{id_col}' has {dup_count} duplicates "
            f"({dup_pct:.2f}% > threshold {max_dup_pct}%)"
        )
        logger.error("✘ DQ failure: %s", msg)
        CONFIG["notify_fn"](f"DQ FAILURE — {msg}")
        raise ValueError(msg)
    logger.info(
        "✔ DQ [%s.%s]: %d duplicates (%.2f%%)", table_name, id_col, dup_count, dup_pct,
    )


def reconcile_row_counts(
    source_count: int,
    target_count: int,
    stage_label: str,
    max_drop_pct: float = 5.0,
) -> None:
    """
    Validate that row-count drop between two stages is within acceptable limits.

    Args:
        source_count: Row count from the upstream stage.
        target_count: Row count from the downstream stage.
        stage_label:  Human-readable label for log messages.
        max_drop_pct: Maximum allowed drop percentage before raising an alert.
    """
    if source_count == 0:
        logger.warning("⚠ Reconciliation skipped for '%s' — source count is 0.", stage_label)
        return

    drop_pct = (source_count - target_count) / source_count * 100
    logger.info(
        "🔄 Reconcile [%s]: source=%d  target=%d  drop=%.2f%%",
        stage_label, source_count, target_count, drop_pct,
    )
    if drop_pct > max_drop_pct:
        msg = (
            f"[{stage_label}] Row-count drop of {drop_pct:.2f}% exceeds "
            f"max allowed {max_drop_pct}%  (source={source_count}, target={target_count})"
        )
        logger.warning("⚠ %s", msg)
        CONFIG["notify_fn"](f"RECONCILIATION WARNING — {msg}")


def check_data_freshness(df: DataFrame, date_col: str, table_name: str, max_lag_days: int = 7) -> None:
    """
    Verify that the most recent record is within max_lag_days of today.

    Args:
        df:           DataFrame containing the date column.
        date_col:     Name of the date/timestamp column to inspect.
        table_name:   Used in log messages.
        max_lag_days: Alert if the latest record is older than this many days.
    """
    latest = df.agg(spark_max(col(date_col))).collect()[0][0]
    if latest is None:
        logger.warning("⚠ [%s] Cannot determine freshness — '%s' is entirely null.", table_name, date_col)
        return
    # Coerce to date for comparison
    if hasattr(latest, "date"):
        latest_date = latest.date()
    else:
        latest_date = latest
    today = datetime.now(timezone.utc).date()
    lag_days = (today - latest_date).days
    if lag_days > max_lag_days:
        msg = (
            f"[{table_name}] Latest '{date_col}' is {lag_days} days old "
            f"(max allowed: {max_lag_days} days)"
        )
        logger.warning("⚠ Freshness: %s", msg)
        CONFIG["notify_fn"](f"FRESHNESS WARNING — {msg}")
    else:
        logger.info("✔ Freshness [%s.%s]: latest=%s  lag=%d day(s)", table_name, date_col, latest_date, lag_days)


# ---------------------------------------------------------------------------
# Audit & Metrics
# ---------------------------------------------------------------------------

AUDIT_LOG: List[Dict] = []   # In-memory audit trail; also persisted to Delta


def record_audit(
    table_name: str,
    stage: str,
    row_count: int,
    status: str = "SUCCESS",
    notes: str = "",
) -> None:
    """
    Append one record to the in-memory audit log.

    Args:
        table_name: Name of the table being processed.
        stage:      Pipeline stage label (e.g. 'bronze_read', 'silver_write').
        row_count:  Number of rows processed at this step.
        status:     'SUCCESS' or 'FAILURE'.
        notes:      Free-text annotation (error messages, warnings, etc.).
    """
    entry = {
        "pipeline_run_ts": datetime.now(timezone.utc).isoformat(),
        "table_name":      table_name,
        "stage":           stage,
        "row_count":       row_count,
        "status":          status,
        "notes":           notes,
    }
    AUDIT_LOG.append(entry)
    logger.info("📝 Audit [%s / %s]: rows=%d  status=%s", table_name, stage, row_count, status)


def persist_audit_log(spark: SparkSession) -> None:
    """
    Write the in-memory audit log to the Delta audit table for long-term storage.

    The audit table schema:
        pipeline_run_ts  STRING   — ISO-8601 timestamp of the pipeline run
        table_name       STRING   — source/target table
        stage            STRING   — pipeline stage
        row_count        LONG     — rows at this stage
        status           STRING   — SUCCESS / FAILURE
        notes            STRING   — optional free-text

    Args:
        spark: Active SparkSession.
    """
    if not AUDIT_LOG:
        logger.info("ℹ Audit log is empty — nothing to persist.")
        return
    audit_df = spark.createDataFrame(AUDIT_LOG)
    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(CONFIG["audit_table"])
    )
    logger.info("✔ Audit log persisted to table '%s' (%d rows)", CONFIG["audit_table"], len(AUDIT_LOG))


# ---
# ## Cell 3 — Bronze Read Functions
# ---

# In[3]:


@timed
@log_step("BRONZE READ")
def read_bronze_sources(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Read all raw Parquet files from the Bronze layer into Spark DataFrames.

    Schema enforcement ensures we catch schema drift early (e.g. renamed columns).
    Each source file is read with the corresponding explicit schema defined in
    SCHEMAS.  If a schema is not defined, the file is read with schema inference.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary mapping source name → raw DataFrame.

    Raises:
        RuntimeError: If any source file cannot be read.
    """
    bronze_path = CONFIG["bronze_path"]
    sources     = CONFIG["sources"]
    raw_dfs: Dict[str, DataFrame] = {}

    for name, filename in sources.items():
        path = f"{bronze_path}/{filename}"
        try:
            schema = SCHEMAS.get(name)
            reader = spark.read.format("parquet")
            if schema:
                reader = reader.schema(schema)
            df = reader.load(path)
            row_count = df.count()
            raw_dfs[name] = df
            record_audit(name, "bronze_read", row_count)
            logger.info("✔ Loaded '%s'  (%d rows) from %s", name, row_count, path)
        except Exception as exc:
            record_audit(name, "bronze_read", 0, status="FAILURE", notes=str(exc))
            logger.error("✘ Failed to load '%s' from %s: %s", name, path, exc)
            raise RuntimeError(f"Cannot read bronze source '{name}': {exc}") from exc

    return raw_dfs


@timed
@log_step("BRONZE DELTA WRITE")
def write_bronze_delta_tables(raw_dfs: Dict[str, DataFrame]) -> None:
    """
    Persist raw DataFrames as Bronze Delta tables (overwrite mode).

    Writing raw data into Delta format provides ACID guarantees, time-travel,
    and a reliable audit baseline before any transformations are applied.

    Args:
        raw_dfs: Dictionary of source name → raw DataFrame (from read_bronze_sources).
    """
    for name, df in raw_dfs.items():
        try:
            df.write.format("delta").mode("overwrite").saveAsTable(name)
            record_audit(name, "bronze_delta_write", df.count())
            logger.info("✔ Bronze Delta table '%s' written.", name)
        except Exception as exc:
            record_audit(name, "bronze_delta_write", 0, status="FAILURE", notes=str(exc))
            logger.error("✘ Failed to write Bronze Delta table '%s': %s", name, exc)
            raise


# ---
# ## Cell 4 — Silver Cleaning Functions
# ---

# In[4]:


@timed
@log_step("SILVER CLEAN — customers")
def clean_customers(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and standardisation rules to the raw customers DataFrame.

    Transformations applied:
        - email      : lower-case + trim whitespace; maps 'EMAIL' column alias.
        - name       : title-case + trim whitespace.
        - gender     : normalise to 'Female' / 'Male' / 'Other'.
        - dob        : coerce mixed date formats (slash-separated) to DateType.
        - location   : title-case.
        - Dedup      : retain first occurrence per customer_id.
        - Drop nulls : rows with null customer_id or email are dropped.

    Data Quality Checks (post-clean):
        - Zero nulls on customer_id.
        - Zero nulls on email.
        - Duplicate rate on customer_id < 1%.
        - Row-count reconciliation against raw.

    Args:
        df: Raw customers DataFrame (from bronze layer).

    Returns:
        Cleaned customers DataFrame ready for Silver layer write.
    """
    raw_count = df.count()

    # ── Anomaly detection: log rows with suspicious data before cleaning ──
    suspicious_email_count = df.filter(
        col("EMAIL").isNull() | ~col("EMAIL").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    ).count()
    if suspicious_email_count > 0:
        logger.warning("⚠ [customers] %d rows have invalid/missing email before cleaning.", suspicious_email_count)

    clean = (
        df
        .withColumn("email",    lower(trim(col("EMAIL"))))
        .withColumn("name",     initcap(trim(col("name"))))
        .withColumn("gender",
            when(lower(col("gender")).isin("f", "female"), "Female")
            .when(lower(col("gender")).isin("m", "male"),  "Male")
            .otherwise("Other")
        )
        .withColumn("dob",      to_date(regexp_replace(col("dob"), "/", "-")))
        .withColumn("location", initcap(col("location")))
        # ── Audit columns ───────────────────────────────────────────────────
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_source_layer", lit("bronze"))
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id", "email"])
    )

    clean_count = clean.count()
    assert_no_critical_nulls(clean, ["customer_id", "email"], "silver_customers",
                             max_null_pct=CONFIG["dq_max_null_pct"])
    assert_no_excessive_duplicates(clean, "customer_id", "silver_customers",
                                   max_dup_pct=CONFIG["dq_max_duplicate_pct"])
    reconcile_row_counts(raw_count, clean_count, "bronze→silver_customers")
    return clean


@timed
@log_step("SILVER CLEAN — orders")
def clean_orders(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and standardisation rules to the raw orders DataFrame.

    Transformations applied:
        - order_date : parse multiple date formats (yyyy/MM/dd, dd-MM-yyyy,
                       yyyyMMdd, yyyy-MM-dd) into a uniform DateType.
        - amount     : cast to DoubleType; null-out negative amounts (anomaly).
        - status     : title-case normalisation.
        - Dedup      : retain first occurrence per order_id.
        - Drop nulls : rows with null customer_id or order_date are dropped.

    Anomaly Detection:
        - Negative amounts are replaced with NULL and logged.
        - Orders with dates before 2000-01-01 are flagged.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned orders DataFrame.
    """
    raw_count = df.count()

    # ── Anomaly detection ────────────────────────────────────────────────────
    neg_count = df.filter(col("amount").cast(DoubleType()) < 0).count()
    if neg_count > 0:
        logger.warning("⚠ [orders] %d rows have negative amounts — setting to NULL.", neg_count)
        CONFIG["notify_fn"](f"[orders] {neg_count} negative amount(s) detected.")

    clean = (
        df
        .withColumn("order_date",
            when(col("order_date").rlike(r"^\d{4}/\d{2}/\d{2}$"),
                 to_date(col("order_date"), "yyyy/MM/dd"))
            .when(col("order_date").rlike(r"^\d{2}-\d{2}-\d{4}$"),
                 to_date(col("order_date"), "dd-MM-yyyy"))
            .when(col("order_date").rlike(r"^\d{8}$"),
                 to_date(col("order_date"), "yyyyMMdd"))
            .otherwise(to_date(col("order_date"), "yyyy-MM-dd"))
        )
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
        .withColumn("status", initcap(col("status")))
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_source_layer", lit("bronze"))
        .dropna(subset=["customer_id", "order_date"])
        .dropDuplicates(["order_id"])
    )

    # Flag suspiciously old dates
    old_dates = clean.filter(col("order_date") < lit("2000-01-01")).count()
    if old_dates > 0:
        logger.warning("⚠ [orders] %d rows have order_date before 2000-01-01.", old_dates)

    clean_count = clean.count()
    assert_no_critical_nulls(clean, ["customer_id", "order_date"], "silver_orders",
                             max_null_pct=CONFIG["dq_max_null_pct"])
    assert_no_excessive_duplicates(clean, "order_id", "silver_orders",
                                   max_dup_pct=CONFIG["dq_max_duplicate_pct"])
    reconcile_row_counts(raw_count, clean_count, "bronze→silver_orders")
    return clean


@timed
@log_step("SILVER CLEAN — payments")
def clean_payments(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and standardisation rules to the raw payments DataFrame.

    Transformations applied:
        - payment_date   : coerce slash-separated dates to DateType.
        - payment_method : title-case; map 'creditcard' → 'Credit Card'.
        - payment_status : title-case.
        - amount         : cast to DoubleType; null-out negatives.
        - Drop nulls     : rows with null customer_id, payment_date, or amount.

    Args:
        df: Raw payments DataFrame.

    Returns:
        Cleaned payments DataFrame.
    """
    raw_count = df.count()

    neg_count = df.filter(col("amount").cast(DoubleType()) < 0).count()
    if neg_count > 0:
        logger.warning("⚠ [payments] %d rows have negative amounts — setting to NULL.", neg_count)
        CONFIG["notify_fn"](f"[payments] {neg_count} negative amount(s) detected.")

    clean = (
        df
        .withColumn("payment_date",
                    to_date(regexp_replace(col("payment_date"), "/", "-")))
        .withColumn("payment_method", initcap(col("payment_method")))
        .replace({"creditcard": "Credit Card"}, subset=["payment_method"])
        .withColumn("payment_status", initcap(col("payment_status")))
        .withColumn("amount",  col("amount").cast(DoubleType()))
        .withColumn("amount",  when(col("amount") < 0, None).otherwise(col("amount")))
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_source_layer", lit("bronze"))
        .dropna(subset=["customer_id", "payment_date", "amount"])
    )

    clean_count = clean.count()
    assert_no_critical_nulls(clean, ["customer_id", "payment_date", "amount"], "silver_payments",
                             max_null_pct=CONFIG["dq_max_null_pct"])
    reconcile_row_counts(raw_count, clean_count, "bronze→silver_payments")
    check_data_freshness(clean, "payment_date", "silver_payments", max_lag_days=7)
    return clean


@timed
@log_step("SILVER CLEAN — support")
def clean_support(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and standardisation rules to the raw support tickets DataFrame.

    Transformations applied:
        - ticket_date       : coerce slash-separated dates to DateType.
        - issue_type        : title-case + trim; map NA/empty to NULL.
        - resolution_status : title-case + trim; map NA/empty to NULL.
        - Dedup             : retain first occurrence per ticket_id.
        - Drop nulls        : rows with null customer_id or ticket_date.

    Args:
        df: Raw support_tickets DataFrame.

    Returns:
        Cleaned support DataFrame.
    """
    raw_count = df.count()

    clean = (
        df
        .withColumn("ticket_date",
                    to_date(regexp_replace(col("ticket_date"), "/", "-")))
        .withColumn("issue_type",        initcap(trim(col("issue_type"))))
        .withColumn("resolution_status", initcap(trim(col("resolution_status"))))
        .replace({"NA": None, "Na": None, "na": None, "n/a": None, "N/A": None, "": None},
                 subset=["issue_type", "resolution_status"])
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_source_layer", lit("bronze"))
        .dropDuplicates(["ticket_id"])
        .dropna(subset=["customer_id", "ticket_date"])
    )

    clean_count = clean.count()
    assert_no_critical_nulls(clean, ["customer_id", "ticket_date"], "silver_support",
                             max_null_pct=CONFIG["dq_max_null_pct"])
    assert_no_excessive_duplicates(clean, "ticket_id", "silver_support",
                                   max_dup_pct=CONFIG["dq_max_duplicate_pct"])
    reconcile_row_counts(raw_count, clean_count, "bronze→silver_support")
    return clean


@timed
@log_step("SILVER CLEAN — web")
def clean_web(df: DataFrame) -> DataFrame:
    """
    Apply cleaning and standardisation rules to the raw web activities DataFrame.

    Transformations applied:
        - session_time : coerce slash-separated dates to DateType.
        - page_viewed  : lower-case normalisation.
        - device_type  : title-case normalisation.
        - Dedup        : retain first occurrence per session_id.
        - Drop nulls   : rows with null customer_id, session_time, or page_viewed.

    Args:
        df: Raw web_activities DataFrame.

    Returns:
        Cleaned web DataFrame.
    """
    raw_count = df.count()

    clean = (
        df
        .withColumn("session_time",
                    to_date(regexp_replace(col("session_time"), "/", "-")))
        .withColumn("page_viewed", lower(col("page_viewed")))
        .withColumn("device_type", initcap(col("device_type")))
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_source_layer", lit("bronze"))
        .dropDuplicates(["session_id"])
        .dropna(subset=["customer_id", "session_time", "page_viewed"])
    )

    clean_count = clean.count()
    assert_no_critical_nulls(clean, ["customer_id", "session_time", "page_viewed"],
                             "silver_web", max_null_pct=CONFIG["dq_max_null_pct"])
    assert_no_excessive_duplicates(clean, "session_id", "silver_web",
                                   max_dup_pct=CONFIG["dq_max_duplicate_pct"])
    reconcile_row_counts(raw_count, clean_count, "bronze→silver_web")
    check_data_freshness(clean, "session_time", "silver_web", max_lag_days=7)
    return clean


# ---
# ## Cell 5 — Silver Write Functions (Full & Incremental)
# ---

# In[5]:


@timed
@log_step("SILVER WRITE")
def write_silver_table(
    df: DataFrame,
    table_name: str,
    id_col: str,
    incremental: bool = False,
) -> None:
    """
    Persist a cleaned DataFrame to a Silver Delta table.

    Supports two modes:

    **Full load** (``incremental=False``):
        Overwrites the existing Silver table on every pipeline run.
        Use for smaller tables or initial loads.

    **Incremental / CDC-ready** (``incremental=True``):
        Uses Delta ``MERGE`` to insert only new records (identified by
        ``id_col``) that do not yet exist in the target table.
        Existing rows are left unchanged (insert-only CDC).
        Extend the MERGE condition to support updates if required.

    Args:
        df:          Cleaned DataFrame to write.
        table_name:  Target Delta table name.
        id_col:      Primary-key column used for deduplication in MERGE.
        incremental: If True, perform a MERGE instead of overwrite.

    Schema Changes:
        Silver tables include two audit columns added during cleaning:
            _silver_loaded_at  TIMESTAMP  — when the row entered the Silver layer
            _source_layer      STRING     — always 'bronze' at this stage
    """
    row_count = df.count()

    if not incremental:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info("✔ Silver table '%s' written (overwrite)  %d rows.", table_name, row_count)
    else:
        # CDC-ready incremental merge: insert rows whose id_col is not yet present
        from delta.tables import DeltaTable  # type: ignore
        try:
            target_table = DeltaTable.forName(spark, table_name)
            (
                target_table.alias("tgt")
                .merge(df.alias("src"), f"tgt.{id_col} = src.{id_col}")
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("✔ Silver table '%s' incrementally merged  %d source rows.", table_name, row_count)
        except Exception:
            # Table does not exist yet — create it on first run
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            logger.info("✔ Silver table '%s' created (first incremental run)  %d rows.", table_name, row_count)

    record_audit(table_name, "silver_write", row_count)


# ---
# ## Cell 6 — Gold Aggregation
# ---

# In[6]:


@timed
@log_step("GOLD AGGREGATE — customer360")
def build_customer360(spark: SparkSession) -> DataFrame:
    """
    Join all Silver tables into a denormalised Customer-360 view for BI reporting.

    Join strategy:
        - Base: silver_customers (one row per customer)
        - LEFT JOIN silver_orders    on customer_id  (1:many → fan-out intended)
        - LEFT JOIN silver_payments  on customer_id
        - LEFT JOIN silver_support   on customer_id
        - LEFT JOIN silver_web       on customer_id

    Note: Because of the 1:many joins this view will contain multiple rows per
    customer (one per order × payment × ticket × session combination).  Use
    appropriate aggregation in Power BI visuals or create additional Gold
    aggregation tables for pre-aggregated KPIs.

    Output columns (with aliases for readability):
        customer_id, name, email, gender, dob, location,
        order_id, order_date, order_amount, order_status,
        payment_method, payment_status, payment_amount,
        ticket_id, issue_type, ticket_date, resolution_status,
        page_viewed, device_type, session_time,
        _gold_loaded_at

    Args:
        spark: Active SparkSession.

    Returns:
        Customer-360 DataFrame.
    """
    cust     = spark.table("silver_customers").alias("c")
    orders   = spark.table("silver_orders").alias("o")
    payments = spark.table("silver_payments").alias("p")
    support  = spark.table("silver_support").alias("s")
    web      = spark.table("silver_web").alias("w")

    customer360 = (
        cust
        .join(orders,   "customer_id", "left")
        .join(payments, "customer_id", "left")
        .join(support,  "customer_id", "left")
        .join(web,      "customer_id", "left")
        .select(
            col("c.customer_id"),
            col("c.name"),
            col("c.email"),
            col("c.gender"),
            col("c.dob"),
            col("c.location"),

            col("o.order_id"),
            col("o.order_date"),
            col("o.amount").alias("order_amount"),
            col("o.status").alias("order_status"),

            col("p.payment_method"),
            col("p.payment_status"),
            col("p.amount").alias("payment_amount"),

            col("s.ticket_id"),
            col("s.issue_type"),
            col("s.ticket_date"),
            col("s.resolution_status"),

            col("w.page_viewed"),
            col("w.device_type"),
            col("w.session_time"),

            current_timestamp().alias("_gold_loaded_at"),
        )
    )

    gold_count = customer360.count()
    record_audit("gold_customer360", "gold_aggregate", gold_count)
    logger.info("✔ Customer-360 built: %d rows.", gold_count)
    return customer360


@timed
@log_step("GOLD WRITE — customer360")
def write_gold_table(df: DataFrame, table_name: str = "gold_customer360") -> None:
    """
    Persist the Gold Customer-360 DataFrame as a Delta table.

    Args:
        df:         Gold DataFrame produced by build_customer360.
        table_name: Target Delta table name (default: 'gold_customer360').
    """
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    row_count = df.count()
    record_audit(table_name, "gold_write", row_count)
    logger.info("✔ Gold table '%s' written: %d rows.", table_name, row_count)


# ---
# ## Cell 7 — Pipeline Orchestrator
# ---

# In[7]:


class BronzeToSilverPipeline:
    """
    End-to-end orchestrator for the Bronze → Silver → Gold medallion pipeline.

    Usage:
        pipeline = BronzeToSilverPipeline(spark)
        pipeline.run()

    The orchestrator:
        1. Reads all Bronze Parquet sources.
        2. Persists them as Bronze Delta tables.
        3. Cleans each table with domain-specific logic.
        4. Validates data quality (nulls, duplicates, row-count reconciliation).
        5. Writes cleaned data to Silver Delta tables.
        6. Builds and writes the Gold Customer-360 aggregation.
        7. Persists the audit log to a Delta table for observability.
        8. Prints a final reconciliation / metrics report.

    Attributes:
        spark:       SparkSession used throughout the pipeline.
        incremental: If True, Silver writes use MERGE instead of overwrite.
    """

    # ── Silver table configuration: (clean_fn, id_col, silver_table_name)
    _SILVER_CONFIG = [
        ("customers", clean_customers, "customer_id", "silver_customers"),
        ("orders",    clean_orders,    "order_id",    "silver_orders"),
        ("payments",  clean_payments,  "payment_id",  "silver_payments"),
        ("support",   clean_support,   "ticket_id",   "silver_support"),
        ("web",       clean_web,       "session_id",  "silver_web"),
    ]

    def __init__(self, spark: SparkSession, incremental: bool = False) -> None:
        self.spark       = spark
        self.incremental = incremental
        self._metrics: Dict[str, int] = {}

    @timed
    def run(self) -> None:
        """Execute the full pipeline end-to-end."""
        pipeline_start = time.perf_counter()
        logger.info("=" * 70)
        logger.info("🚀 BronzeToSilverPipeline — START  (incremental=%s)", self.incremental)
        logger.info("=" * 70)

        try:
            # ── 1. Read Bronze ───────────────────────────────────────────────
            raw_dfs = read_bronze_sources(self.spark)

            # ── 2. Write Bronze Delta ────────────────────────────────────────
            write_bronze_delta_tables(raw_dfs)

            # ── 3. Clean → 4. Validate → 5. Write Silver ────────────────────
            for source_name, clean_fn, id_col, silver_table in self._SILVER_CONFIG:
                try:
                    source_df   = raw_dfs[source_name]
                    cleaned_df  = clean_fn(source_df)
                    write_silver_table(
                        cleaned_df, silver_table, id_col,
                        incremental=self.incremental,
                    )
                    self._metrics[silver_table] = cleaned_df.count()
                except Exception as exc:
                    record_audit(source_name, "silver_pipeline", 0,
                                 status="FAILURE", notes=str(exc))
                    logger.error("✘ Pipeline step failed for '%s': %s", source_name, exc)
                    raise

            # ── 6. Gold Aggregation ──────────────────────────────────────────
            gold_df = build_customer360(self.spark)
            write_gold_table(gold_df)
            self._metrics["gold_customer360"] = gold_df.count()

            # ── 7. Audit log ─────────────────────────────────────────────────
            persist_audit_log(self.spark)

            # ── 8. Reconciliation report ─────────────────────────────────────
            self._print_reconciliation_report(time.perf_counter() - pipeline_start)

        except Exception as exc:
            logger.error("✘ Pipeline FAILED: %s", exc)
            CONFIG["notify_fn"](f"PIPELINE FAILURE: {exc}")
            raise

    def _print_reconciliation_report(self, elapsed_seconds: float) -> None:
        """Print a summary reconciliation report to the notebook output."""
        separator = "─" * 60
        print(separator)
        print("  PIPELINE RECONCILIATION REPORT")
        print(separator)
        print(f"  Run timestamp : {datetime.now(timezone.utc).isoformat()}")
        print(f"  Elapsed time  : {elapsed_seconds:.2f}s")
        print(separator)
        print(f"  {'Table':<30}  {'Row Count':>12}")
        print(separator)
        for table, count in self._metrics.items():
            print(f"  {table:<30}  {count:>12,}")
        print(separator)
        print(f"  Total audit entries : {len(AUDIT_LOG)}")
        print(separator)


# ---
# ## Cell 8 — Run the Pipeline
# ---

# In[8]:


# Execute the full pipeline.
# Set incremental=True to enable CDC-ready MERGE mode for Silver tables.
pipeline = BronzeToSilverPipeline(spark, incremental=CONFIG["incremental_mode"])
pipeline.run()


# ---
# ## Cell 9 — Ad-hoc Data Profiling & Quality Report
# ---

# In[9]:


# Run optional data profiling on Silver tables for exploratory analysis.
# This cell can be run independently after the pipeline has completed.

silver_tables = [
    ("silver_customers", "customer_id"),
    ("silver_orders",    "order_id"),
    ("silver_payments",  "payment_id"),
    ("silver_support",   "ticket_id"),
    ("silver_web",       "session_id"),
]

profiling_results = []
for table_name, id_col in silver_tables:
    try:
        df = spark.table(table_name)
        profile = profile_dataframe(df, table_name)
        profiling_results.append(profile)
        print(f"\n{'─'*50}")
        print(f"  📊 {table_name}")
        print(f"  Rows        : {profile['row_count']:,}")
        print(f"  Completeness: {profile['completeness_pct']:.1f}%")
        print(f"  Null summary:")
        for col_name, pct in profile["null_pct"].items():
            if pct > 0:
                print(f"    {col_name:<30} {pct:.2f}%")
    except Exception as exc:
        logger.error("Could not profile '%s': %s", table_name, exc)


# ---
# ## Cell 10 — Preview Silver & Gold Tables
# ---

# In[10]:


# Preview cleaned Silver tables.
# (display() is a Microsoft Fabric / Databricks notebook built-in.)
display(spark.table("silver_customers").limit(6))
display(spark.table("silver_orders").limit(5))
display(spark.table("silver_payments").limit(5))
display(spark.table("silver_support").limit(5))
display(spark.table("silver_web").limit(5))

# Preview Gold Customer-360
display(spark.table("gold_customer360").limit(10))

