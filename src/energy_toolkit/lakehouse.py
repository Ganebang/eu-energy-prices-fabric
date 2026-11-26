"""Delta Lake / OneLake helpers used by every notebook."""
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def land_raw_payload(notebookutils, landing_path: str, raw_text: str) -> str:
    """Write the untouched API payload to OneLake Files (bronze landing zone)."""
    notebookutils.fs.put(landing_path, raw_text, True)
    return landing_path


def merge_upsert(spark: SparkSession, source_df: DataFrame, target_table: str,
                 key_columns: list[str], compare_columns: list[str]) -> dict:
    """Idempotent upsert: insert new keys, update rows whose values changed.

    Re-running the same load produces no duplicates and no useless rewrites.
    """
    target = DeltaTable.forName(spark, target_table)
    join_condition = " AND ".join(f"t.{c} = s.{c}" for c in key_columns)
    change_condition = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compare_columns)
    (
        target.alias("t")
        .merge(source_df.alias("s"), join_condition)
        .whenMatchedUpdateAll(condition=change_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = (
        target.history(1)
        .select(F.col("operationMetrics").alias("m"))
        .collect()[0]["m"]
    )
    return {
        "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }


def log_run(spark: SparkSession, pipeline_run_id: str, notebook_name: str, entity_name: str,
            zone_code: str, status: str, rows_read: int, rows_inserted: int,
            rows_updated: int, started_at: datetime, message: str = "") -> None:
    """Append one line to ops.ingestion_log (observability / audit trail)."""
    schema = (
        "pipeline_run_id STRING, notebook_name STRING, entity_name STRING, zone_code STRING, "
        "status STRING, rows_read BIGINT, rows_inserted BIGINT, rows_updated BIGINT, "
        "started_at_utc TIMESTAMP, ended_at_utc TIMESTAMP, message STRING"
    )
    record = [(pipeline_run_id, notebook_name, entity_name, zone_code, status, rows_read,
               rows_inserted, rows_updated, started_at, utc_now(), message[:4000])]
    spark.createDataFrame(record, schema).write.mode("append").saveAsTable("ops.ingestion_log")


def lakehouse_files_root(notebookutils, lakehouse_name: str) -> str:
    """abfss:// root of the Files area of a lakehouse (works from any notebook)."""
    properties = notebookutils.lakehouse.get(lakehouse_name)["properties"]
    return f"{properties['abfsPath']}/Files"
