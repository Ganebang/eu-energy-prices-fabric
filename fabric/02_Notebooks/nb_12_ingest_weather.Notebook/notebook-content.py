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

# # nb_12_ingest_weather  *(enrichissement)*
# **Rôle** : ingérer la météo horaire (observée **et prévue**) au point représentatif de la zone de marché.
# Source : Open-Meteo (CC BY 4.0, sans clé).
# 
# | Variable | Impact sur le prix |
# |---|---|
# | `shortwave_radiation_wm2` | production solaire ⇒ creux de prix à midi |
# | `wind_speed_100m_kmh` | vent à hauteur de nacelle ⇒ production éolienne |
# | `temperature_2m_c` | chauffage / climatisation ⇒ demande |
# | `cloud_cover_pct` | nébulosité ⇒ solaire |
# 
# Les **prévisions** (`is_forecast = true`) sont la base d'un futur modèle de prévision de prix (piste ML).

# PARAMETERS CELL ********************

zone_code = "FR"
past_days = 3
forecast_days = 3
pipeline_run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from energy_toolkit.http_client import build_session
from energy_toolkit.lakehouse import (land_raw_payload, lakehouse_files_root, log_run,
                                      merge_upsert, utc_now)
from energy_toolkit.reference import get_zone
from energy_toolkit.sources import fetch_hourly_weather

NOTEBOOK_NAME = "nb_12_ingest_weather"
ENTITY_NAME = "weather_hourly"
spark.conf.set("spark.sql.session.timeZone", "UTC")

zone = get_zone(zone_code)
run_started_at = utc_now()
files_root = lakehouse_files_root(notebookutils, "lh_energy")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weather_rows, raw_payload = fetch_hourly_weather(
    build_session(), zone.latitude, zone.longitude, int(past_days), int(forecast_days)
)
landing_path = (f"{files_root}/bronze/open_meteo/weather_hourly/zone_code={zone.zone_code}/"
                f"{run_started_at:%Y%m%dT%H%M%S}.json")
land_raw_payload(notebookutils, landing_path, raw_payload)
rows_read = len(weather_rows)
print(f"{zone.zone_code}: {rows_read} hourly rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weather_schema = T.StructType([
    T.StructField("interval_start_utc", T.TimestampType(), False),
    T.StructField("temperature_2m_c", T.DoubleType(), True),
    T.StructField("cloud_cover_pct", T.DoubleType(), True),
    T.StructField("wind_speed_100m_kmh", T.DoubleType(), True),
    T.StructField("shortwave_radiation_wm2", T.DoubleType(), True),
])
bronze_weather_df = (
    spark.createDataFrame(weather_rows, weather_schema)
    .withColumn("zone_code", F.lit(zone.zone_code))
    .withColumn("source_file", F.lit(landing_path))
    .withColumn("ingested_at_utc", F.lit(run_started_at).cast("timestamp"))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .select("zone_code", "interval_start_utc", "temperature_2m_c", "cloud_cover_pct",
            "wind_speed_100m_kmh", "shortwave_radiation_wm2", "source_file",
            "ingested_at_utc", "pipeline_run_id")
)
bronze_weather_df.write.mode("append").saveAsTable("bronze.weather_hourly_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver : une prévision est remplacée par l'observation dès qu'elle devient disponible (MERGE)

# CELL ********************

silver_weather_df = (
    bronze_weather_df
    .dropDuplicates(["zone_code", "interval_start_utc"])
    .withColumn("is_forecast", F.col("interval_start_utc") > F.lit(run_started_at).cast("timestamp"))
    .withColumn("updated_at_utc", F.lit(run_started_at).cast("timestamp"))
    .select("zone_code", "interval_start_utc", "temperature_2m_c", "cloud_cover_pct",
            "wind_speed_100m_kmh", "shortwave_radiation_wm2", "is_forecast",
            "updated_at_utc", "pipeline_run_id")
)
merge_metrics = merge_upsert(
    spark, silver_weather_df, "silver.weather_hourly",
    key_columns=["zone_code", "interval_start_utc"],
    compare_columns=["temperature_2m_c", "cloud_cover_pct", "wind_speed_100m_kmh",
                     "shortwave_radiation_wm2", "is_forecast"],
)
log_run(spark, pipeline_run_id, NOTEBOOK_NAME, ENTITY_NAME, zone.zone_code, "SUCCEEDED",
        rows_read, merge_metrics["rows_inserted"], merge_metrics["rows_updated"], run_started_at)
print(merge_metrics)
notebookutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "zone_code": zone.zone_code,
                                        "rows_read": rows_read, **merge_metrics}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
