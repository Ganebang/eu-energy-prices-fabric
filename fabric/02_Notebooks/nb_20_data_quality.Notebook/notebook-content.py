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

# # nb_20_data_quality
# **Rôle** : contrôler la qualité de la couche silver **avant** de publier la couche gold.
# 
# | Contrôle | Sévérité | Règle |
# |---|---|---|
# | `price_completeness` | ERROR | nombre d'intervalles du jour = durée du jour local / résolution (gère 23 h / 25 h aux changements d'heure) |
# | `price_within_market_limits` | ERROR | prix entre −500 et +4 000 €/MWh (bornes harmonisées SDAC) |
# | `next_day_published` | WARNING | après 14h00 CET, les prix de J+1 doivent être présents |
# | `weather_not_null` | WARNING | pas de valeur météo manquante |
# 
# Les résultats sont historisés dans `ops.data_quality_result`. Une erreur bloquante fait échouer le notebook,
# donc l'activité du pipeline, donc la procédure gold ne s'exécute pas : **on ne publie jamais de données fausses**.

# PARAMETERS CELL ********************

lookback_days = 3
fail_on_error = True
pipeline_run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

from energy_toolkit.lakehouse import utc_now

spark.conf.set("spark.sql.session.timeZone", "UTC")
check_run_at = utc_now()
SDAC_MIN_PRICE, SDAC_MAX_PRICE = -500.0, 4000.0

zones_df = spark.table("silver.dim_bidding_zone").select("zone_code", "market_timezone")
recent_prices_df = (
    spark.table("silver.day_ahead_price")
    .where(F.col("delivery_date_local") >= F.date_sub(F.current_date(), int(lookback_days)))
    .join(zones_df, "zone_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Complétude : intervalles attendus vs reçus par zone et par jour

# CELL ********************

local_day_start_utc = F.to_utc_timestamp(F.col("delivery_date_local").cast("timestamp"), F.col("market_timezone"))
local_day_end_utc = F.to_utc_timestamp(F.date_add("delivery_date_local", 1).cast("timestamp"), F.col("market_timezone"))

completeness_df = (
    recent_prices_df
    .groupBy("zone_code", "delivery_date_local", "market_timezone")
    .agg(F.count("*").alias("observed_intervals"),
         F.min("resolution_minutes").alias("resolution_minutes"))
    .withColumn("expected_intervals",
                (F.unix_timestamp(local_day_end_utc) - F.unix_timestamp(local_day_start_utc))
                / 60 / F.col("resolution_minutes"))
    .select(
        F.lit("price_completeness").alias("check_name"),
        F.lit("silver.day_ahead_price").alias("table_name"),
        "zone_code",
        F.col("delivery_date_local").alias("check_date"),
        F.lit("ERROR").alias("severity"),
        (F.col("observed_intervals") == F.col("expected_intervals")).alias("passed"),
        F.col("observed_intervals").cast("double").alias("observed_value"),
        F.col("expected_intervals").cast("double").alias("expected_value"),
        F.concat(F.lit("resolution="), F.col("resolution_minutes"), F.lit(" min")).alias("details"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Bornes de marché

# CELL ********************

limits_df = (
    recent_prices_df
    .groupBy("zone_code", "delivery_date_local")
    .agg(F.sum(F.when((F.col("price_eur_mwh") < SDAC_MIN_PRICE) |
                      (F.col("price_eur_mwh") > SDAC_MAX_PRICE), 1).otherwise(0)).alias("out_of_range"),
         F.min("price_eur_mwh").alias("min_price"), F.max("price_eur_mwh").alias("max_price"))
    .select(
        F.lit("price_within_market_limits").alias("check_name"),
        F.lit("silver.day_ahead_price").alias("table_name"),
        "zone_code",
        F.col("delivery_date_local").alias("check_date"),
        F.lit("ERROR").alias("severity"),
        (F.col("out_of_range") == 0).alias("passed"),
        F.col("out_of_range").cast("double").alias("observed_value"),
        F.lit(0.0).alias("expected_value"),
        F.format_string("min=%.2f max=%.2f EUR/MWh", "min_price", "max_price").alias("details"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Fraîcheur : prix du lendemain publiés ?

# CELL ********************

paris_now = F.from_utc_timestamp(F.current_timestamp(), "Europe/Paris")
tomorrow_df = (
    zones_df.alias("z")
    .join(spark.table("silver.day_ahead_price")
          .where(F.col("delivery_date_local") == F.date_add(F.to_date(paris_now), 1))
          .groupBy("zone_code").count().alias("p"), "zone_code", "left")
    .select(
        F.lit("next_day_published").alias("check_name"),
        F.lit("silver.day_ahead_price").alias("table_name"),
        "zone_code",
        F.date_add(F.to_date(paris_now), 1).alias("check_date"),
        F.lit("WARNING").alias("severity"),
        ((F.coalesce(F.col("count"), F.lit(0)) > 0) | (F.hour(paris_now) < 14)).alias("passed"),
        F.coalesce(F.col("count"), F.lit(0)).cast("double").alias("observed_value"),
        F.lit(1.0).alias("expected_value"),
        F.lit("checked only after 14:00 Europe/Paris").alias("details"),
    )
    # only zones that are actually loaded by the pipeline
    .join(spark.table("silver.day_ahead_price").select("zone_code").distinct(), "zone_code")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Météo complète

# CELL ********************

weather_df = (
    spark.table("silver.weather_hourly")
    .where(F.col("interval_start_utc") >= F.date_sub(F.current_date(), int(lookback_days)))
    .groupBy("zone_code", F.to_date("interval_start_utc").alias("check_date"))
    .agg(F.sum(F.when(F.col("temperature_2m_c").isNull() | F.col("wind_speed_100m_kmh").isNull() |
                      F.col("shortwave_radiation_wm2").isNull(), 1).otherwise(0)).alias("null_rows"))
    .select(
        F.lit("weather_not_null").alias("check_name"),
        F.lit("silver.weather_hourly").alias("table_name"),
        "zone_code", "check_date",
        F.lit("WARNING").alias("severity"),
        (F.col("null_rows") == 0).alias("passed"),
        F.col("null_rows").cast("double").alias("observed_value"),
        F.lit(0.0).alias("expected_value"),
        F.lit("").alias("details"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quality_results_df = (
    completeness_df.unionByName(limits_df).unionByName(tomorrow_df).unionByName(weather_df)
    .withColumn("check_run_at_utc", F.lit(check_run_at).cast("timestamp"))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    .select("check_run_at_utc", "pipeline_run_id", "check_name", "table_name", "zone_code",
            "check_date", "severity", "passed", "observed_value", "expected_value", "details")
).cache()

quality_results_df.write.mode("append").saveAsTable("ops.data_quality_result")
display(quality_results_df.orderBy("passed", "severity", "check_name", "zone_code", "check_date"))

blocking_failures = quality_results_df.where((F.col("severity") == "ERROR") & ~F.col("passed")).count()
print(f"Checks: {quality_results_df.count()} | blocking failures: {blocking_failures}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Synchroniser le SQL analytics endpoint avant la couche gold
# Le Warehouse lit `lh_energy.silver.*` **via le SQL analytics endpoint** du Lakehouse, dont les métadonnées sont
# synchronisées de façon asynchrone avec les commits Delta. Sans synchronisation explicite, la procédure gold peut
# lire une version antérieure des tables (cas réellement rencontré : les 2 dernières zones chargées manquaient).
# L'API `refreshMetadata` force cette synchronisation et rend la chaîne déterministe.

# CELL ********************

import sempy.fabric as fabric

if not blocking_failures:
    rest_client = fabric.FabricRestClient()
    workspace_id = fabric.get_workspace_id()
    lakehouse_id = fabric.resolve_item_id("lh_energy", "Lakehouse", workspace_id)
    sql_endpoint_id = (rest_client.get(f"v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
                       .json()["properties"]["sqlEndpointProperties"]["id"])
    sync = rest_client.post(f"v1/workspaces/{workspace_id}/sqlEndpoints/{sql_endpoint_id}/refreshMetadata", json={})
    print("SQL endpoint metadata refresh:", sync.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if blocking_failures and fail_on_error:
    raise ValueError(f"{blocking_failures} blocking data-quality check(s) failed - see ops.data_quality_result")
notebookutils.notebook.exit(f"DQ_OK:{blocking_failures}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
