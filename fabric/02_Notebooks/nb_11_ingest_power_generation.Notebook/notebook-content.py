# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8289c3c9-cb7c-44b6-8634-fb4751b9b4c0",
# META       "default_lakehouse_name": "lh_energy",
# META       "default_lakehouse_workspace_id": "b2058716-e276-4cdb-a2e1-ebc7df94f6d8"
# META     },
# META     "environment": {
# META       "environmentId": "5c63172f-f029-ba83-4731-1bd01ab86eb0",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_11_ingest_power_generation
# 
# Ingestion de la production électrique publique par filière et de la consommation du pays rattaché à la zone de marché.
# 
# | | |
# |---|---|
# | Couche | Bronze → Silver |
# | Source | `api.energy-charts.info/public_power` (Fraunhofer ISE, licence CC BY 4.0) |
# | Écrit | `Files/bronze/energy_charts/public_power/`, `bronze.power_generation_raw`, `silver.power_generation`, `ops.ingestion_log` |
# | Déclenchement | `pl_energy_daily`, après `nb_10` pour chaque zone |
# | Ré-exécutable | oui, `MERGE` sur (`country_code`, `interval_start_utc`, `production_type`) |
# 
# Paramètres : `zone_code`, `start_date`, `end_date`, `pipeline_run_id`. La fenêtre par défaut va de J-2 à aujourd'hui, la production n'étant publiée qu'au fil de l'eau.
# 
# Ces données servent à expliquer les prix : la production solaire et éolienne pèse directement sur le prix de chaque heure.

# PARAMETERS CELL ********************

zone_code = "FR"
start_date = ""
end_date = ""
pipeline_run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

from energy_toolkit.dates import resolve_window, split_into_chunks
from energy_toolkit.http_client import build_session
from energy_toolkit.lakehouse import (land_raw_payload, lakehouse_files_root, log_run,
                                      merge_upsert, utc_now)
from energy_toolkit.reference import get_zone
from energy_toolkit.sources import fetch_public_power

NOTEBOOK_NAME = "nb_11_ingest_power_generation"
ENTITY_NAME = "power_generation"
API_PAUSE_SECONDS = 2  # pause between two API calls
spark.conf.set("spark.sql.session.timeZone", "UTC")

zone = get_zone(zone_code)
country_code = zone.energy_charts_country
# generation is only published up to "now": stop the window today
window_start, window_end = resolve_window(start_date, end_date, default_days_back=2, default_days_ahead=0)
run_started_at = utc_now()
files_root = lakehouse_files_root(notebookutils, "lh_energy")
print(f"Country {country_code} (zone {zone.zone_code}) | window {window_start} -> {window_end}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Extraction
# 
# Appels par tranches de 14 jours, dépôt du JSON brut dans `Files/bronze`.

# CELL ********************

# Energy-Charts is a free public API: be polite (sequential calls, pause, long backoff)
http_session = build_session(total_retries=8, backoff_factor=3.0)
generation_rows: list[tuple] = []
for chunk_start, chunk_end in split_into_chunks(window_start, window_end, max_days=14):
    chunk_rows, raw_payload = fetch_public_power(http_session, country_code, chunk_start, chunk_end)
    landing_path = (
        f"{files_root}/bronze/energy_charts/public_power/country_code={country_code}/"
        f"{chunk_start:%Y-%m-%d}_{chunk_end:%Y-%m-%d}_{run_started_at:%Y%m%dT%H%M%S}.json"
    )
    land_raw_payload(notebookutils, landing_path, raw_payload)
    generation_rows.extend((*row, landing_path) for row in chunk_rows)
    time.sleep(API_PAUSE_SECONDS)

rows_read = len(generation_rows)
print(f"Rows read: {rows_read}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if rows_read == 0:
    log_run(spark, pipeline_run_id, NOTEBOOK_NAME, ENTITY_NAME, zone.zone_code, "NO_DATA",
            0, 0, 0, run_started_at, f"No public_power data for {country_code}")
    notebookutils.notebook.exit(json.dumps({"status": "NO_DATA", "country_code": country_code}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Bronze

# CELL ********************

generation_schema = T.StructType([
    T.StructField("interval_start_utc", T.TimestampType(), False),
    T.StructField("production_type", T.StringType(), False),
    T.StructField("power_value", T.DoubleType(), True),
    T.StructField("source_file", T.StringType(), True),
])
bronze_generation_df = (
    spark.createDataFrame(generation_rows, generation_schema)
    .withColumn("country_code", F.lit(country_code))
    .withColumn("ingested_at_utc", F.lit(run_started_at).cast("timestamp"))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .select("country_code", "interval_start_utc", "production_type", "power_value",
            "source_file", "ingested_at_utc", "pipeline_run_id")
)
bronze_generation_df.write.mode("append").saveAsTable("bronze.power_generation_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Silver
# 
# Chaque filière est rangée dans une catégorie (`Renewable`, `Nuclear`, `Fossil`, `Storage`, `Load`, `Indicator`, `Other`) par une expression `when/otherwise`. Les séries exprimées en pourcentage (parts de production) reçoivent l'unité `%`, les autres `MW`.

# CELL ********************

production_type_lower = F.lower("production_type")
production_category = (
    F.when(production_type_lower.contains("share"), "Indicator")
    .when(production_type_lower.isin("load", "residual load"), "Load")
    .when(production_type_lower.contains("pumped storage") | production_type_lower.contains("battery"), "Storage")
    .when(production_type_lower.contains("cross border"), "Other")
    .when(production_type_lower.rlike("solar|wind|hydro|biomass|geothermal|renewable"), "Renewable")
    .when(production_type_lower.contains("nuclear"), "Nuclear")
    .when(production_type_lower.rlike("fossil|gas|coal|lignite|oil"), "Fossil")
    .otherwise("Other")
)

silver_generation_df = (
    bronze_generation_df
    .dropDuplicates(["country_code", "interval_start_utc", "production_type"])
    .withColumn("production_category", production_category)
    .withColumn("value_unit", F.when(production_type_lower.contains("share"), "%").otherwise("MW"))
    .withColumn("updated_at_utc", F.lit(run_started_at).cast("timestamp"))
    .select("country_code", "interval_start_utc", "production_type", "production_category",
            "power_value", "value_unit", "updated_at_utc", "pipeline_run_id")
)

merge_metrics = merge_upsert(
    spark, silver_generation_df, "silver.power_generation",
    key_columns=["country_code", "interval_start_utc", "production_type"],
    compare_columns=["power_value"],
)
log_run(spark, pipeline_run_id, NOTEBOOK_NAME, ENTITY_NAME, zone.zone_code, "SUCCEEDED",
        rows_read, merge_metrics["rows_inserted"], merge_metrics["rows_updated"], run_started_at)
print(merge_metrics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("silver.power_generation")
    .where((F.col("country_code") == country_code) & (F.col("value_unit") == "MW"))
    .groupBy("production_category", "production_type")
    .agg(F.round(F.avg("power_value"), 0).alias("avg_mw"))
    .orderBy("production_category", F.desc("avg_mw"))
)
notebookutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "country_code": country_code,
                                        "rows_read": rows_read, **merge_metrics}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
