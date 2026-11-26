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

# # nb_10_ingest_day_ahead_prices
# 
# Ingestion des prix day-ahead EPEX Spot d'une zone de marché, de l'API jusqu'à la couche silver.
# 
# | | |
# |---|---|
# | Couche | Bronze → Silver |
# | Source | `api.energy-charts.info/price` (Fraunhofer ISE, licence CC BY 4.0) |
# | Écrit | `Files/bronze/energy_charts/day_ahead_price/`, `bronze.day_ahead_price_raw`, `silver.day_ahead_price`, `ops.ingestion_log` |
# | Déclenchement | `pl_energy_daily`, une exécution par zone |
# | Ré-exécutable | oui, `MERGE` sur (`zone_code`, `interval_start_utc`) |
# 
# Paramètres (transmis par le pipeline) :
# - `zone_code` : code de zone Energy-Charts, par exemple `FR`, `BE`, `DE-LU`, `NL`, `ES` ;
# - `start_date`, `end_date` : fenêtre `yyyy-MM-dd` ; vides, la fenêtre va de J-1 à J+1 (UTC) ;
# - `pipeline_run_id` : identifiant d'exécution, `manual` en lancement interactif.
# 
# Les prix du lendemain sont publiés vers 12h45 (heure de Paris). Le notebook peut être relancé autant de fois que nécessaire : les intervalles déjà chargés ne sont mis à jour que si le prix a changé.

# PARAMETERS CELL ********************

zone_code = "FR"            # Energy-Charts bidding zone code: FR, BE, DE-LU, NL, ES ...
start_date = ""             # yyyy-MM-dd ; empty = yesterday (UTC)
end_date = ""               # yyyy-MM-dd ; empty = tomorrow (UTC)
pipeline_run_id = "manual"  # injected by the pipeline: @pipeline().RunId

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from energy_toolkit.dates import resolve_window, split_into_chunks
from energy_toolkit.http_client import build_session
from energy_toolkit.lakehouse import (land_raw_payload, lakehouse_files_root, log_run,
                                      merge_upsert, utc_now)
from energy_toolkit.reference import get_zone
from energy_toolkit.sources import fetch_day_ahead_prices

NOTEBOOK_NAME = "nb_10_ingest_day_ahead_prices"
ENTITY_NAME = "day_ahead_price"
API_PAUSE_SECONDS = 2  # pause between two API calls
spark.conf.set("spark.sql.session.timeZone", "UTC")

zone = get_zone(zone_code)
window_start, window_end = resolve_window(start_date, end_date)
run_started_at = utc_now()
files_root = lakehouse_files_root(notebookutils, "lh_energy")
print(f"Zone {zone.zone_code} ({zone.country_name}) | window {window_start} -> {window_end} | run {pipeline_run_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Extraction
# 
# Appels séquentiels par tranches de 31 jours au plus, avec une pause entre deux appels. Chaque réponse est déposée telle quelle dans `Files/bronze`, ce qui permet de recharger une période sans rappeler l'API.

# CELL ********************

# Energy-Charts is a free public API: be polite (sequential calls, pause, long backoff)
http_session = build_session(total_retries=8, backoff_factor=3.0)
price_rows: list[tuple] = []

for chunk_start, chunk_end in split_into_chunks(window_start, window_end):
    chunk_rows, raw_payload = fetch_day_ahead_prices(http_session, zone.zone_code, chunk_start, chunk_end)
    landing_path = (
        f"{files_root}/bronze/energy_charts/day_ahead_price/zone_code={zone.zone_code}/"
        f"{chunk_start:%Y-%m-%d}_{chunk_end:%Y-%m-%d}_{run_started_at:%Y%m%dT%H%M%S}.json"
    )
    land_raw_payload(notebookutils, landing_path, raw_payload)
    price_rows.extend((*row, landing_path) for row in chunk_rows)
    print(f"  {chunk_start} -> {chunk_end}: {len(chunk_rows)} intervals")
    time.sleep(API_PAUSE_SECONDS)

rows_read = len(price_rows)
print(f"Total intervals read: {rows_read}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if rows_read == 0:
    log_run(spark, pipeline_run_id, NOTEBOOK_NAME, ENTITY_NAME, zone.zone_code, "NO_DATA",
            0, 0, 0, run_started_at, "API returned no price for the window")
    notebookutils.notebook.exit(json.dumps({"status": "NO_DATA", "zone_code": zone.zone_code}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Bronze
# 
# Ajout des lignes brutes avec les colonnes de lignage (`source_file`, `ingested_at_utc`, `pipeline_run_id`).

# CELL ********************

bronze_schema = T.StructType([
    T.StructField("interval_start_utc", T.TimestampType(), False),
    T.StructField("price_eur_mwh", T.DoubleType(), True),
    T.StructField("unit", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
])

bronze_price_df = (
    spark.createDataFrame(price_rows, bronze_schema)
    .withColumn("zone_code", F.lit(zone.zone_code))
    .withColumn("ingested_at_utc", F.lit(run_started_at).cast("timestamp"))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .select("zone_code", "interval_start_utc", "price_eur_mwh", "unit",
            "source_file", "ingested_at_utc", "pipeline_run_id")
)
bronze_price_df.write.mode("append").saveAsTable("bronze.day_ahead_price_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Silver
# 
# - La résolution (15 ou 60 minutes) est déduite de l'écart avec l'intervalle voisin. Le même code traite donc les données antérieures et postérieures au passage d'EPEX au pas de 15 minutes (1er octobre 2025). Elle est plafonnée à 60 minutes pour qu'un trou dans les données ne crée pas un intervalle de plusieurs heures.
# - L'heure locale et la date de livraison sont calculées dans le fuseau du marché, ce qui gère les changements d'heure.
# - `price_ct_kwh` = `price_eur_mwh` / 10.

# CELL ********************

interval_order = Window.partitionBy("zone_code").orderBy("interval_start_utc")
seconds_to_next = F.unix_timestamp(F.lead("interval_start_utc").over(interval_order)) - F.unix_timestamp("interval_start_utc")
seconds_from_previous = F.unix_timestamp("interval_start_utc") - F.unix_timestamp(F.lag("interval_start_utc").over(interval_order))

silver_price_df = (
    bronze_price_df
    .dropDuplicates(["zone_code", "interval_start_utc"])
    .withColumn("resolution_minutes",
                (F.coalesce(seconds_to_next, seconds_from_previous, F.lit(3600)) / 60).cast("int"))
    # a gap in the data must not create a 3-hour interval: cap at 60 minutes
    .withColumn("resolution_minutes", F.least(F.col("resolution_minutes"), F.lit(60)))
    .withColumn("interval_end_utc",
                F.timestamp_seconds(F.unix_timestamp("interval_start_utc") + F.col("resolution_minutes") * 60))
    .withColumn("interval_start_local", F.from_utc_timestamp("interval_start_utc", zone.market_timezone))
    .withColumn("delivery_date_local", F.to_date("interval_start_local"))
    .withColumn("price_ct_kwh", F.round(F.col("price_eur_mwh") / 10, 4))
    .withColumn("updated_at_utc", F.lit(run_started_at).cast("timestamp"))
    .select("zone_code", "interval_start_utc", "interval_end_utc", "resolution_minutes",
            "interval_start_local", "delivery_date_local", "price_eur_mwh", "price_ct_kwh",
            "updated_at_utc", "pipeline_run_id")
)

merge_metrics = merge_upsert(
    spark, silver_price_df, "silver.day_ahead_price",
    key_columns=["zone_code", "interval_start_utc"],
    compare_columns=["price_eur_mwh", "resolution_minutes"],
)
print(merge_metrics)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Journalisation et sortie
# 
# Le notebook écrit une ligne dans `ops.ingestion_log` et renvoie un JSON au pipeline (`status`, `rows_read`, `rows_inserted`, `rows_updated`), lisible via `@activity('Ingest day-ahead prices').output.result.exitValue`.

# CELL ********************

log_run(spark, pipeline_run_id, NOTEBOOK_NAME, ENTITY_NAME, zone.zone_code, "SUCCEEDED",
        rows_read, merge_metrics["rows_inserted"], merge_metrics["rows_updated"], run_started_at)

display(
    spark.table("silver.day_ahead_price")
    .where(F.col("zone_code") == zone.zone_code)
    .groupBy("delivery_date_local")
    .agg(F.count("*").alias("intervals"),
         F.round(F.min("price_ct_kwh"), 2).alias("min_ct_kwh"),
         F.round(F.avg("price_ct_kwh"), 2).alias("avg_ct_kwh"),
         F.round(F.max("price_ct_kwh"), 2).alias("max_ct_kwh"))
    .orderBy(F.desc("delivery_date_local"))
    .limit(10)
)

notebookutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "zone_code": zone.zone_code,
                                        "rows_read": rows_read, **merge_metrics}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
