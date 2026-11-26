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

# # nb_00_setup_lakehouse
# 
# Création des schémas et des tables Delta du lakehouse `lh_energy`.
# 
# | | |
# |---|---|
# | Couche | Bronze, Silver, Ops |
# | Écrit | schémas `bronze`, `silver`, `ops` et leurs tables |
# | Déclenchement | manuel, une fois par environnement |
# | Ré-exécutable | oui, toutes les instructions utilisent `IF NOT EXISTS` |
# 
# Organisation du lakehouse :
# 
# | Schéma | Contenu | Mode d'écriture |
# |---|---|---|
# | `bronze` | données telles que renvoyées par les API, avec les métadonnées d'ingestion | ajout seul |
# | `silver` | données typées et dédoublonnées, une ligne par clé métier | `MERGE` |
# | `ops` | journal d'exécution et résultats des contrôles qualité | ajout seul |
# 
# Les tables sont déclarées en DDL plutôt que créées par `saveAsTable` : les types, les commentaires de colonnes et les propriétés Delta sont ainsi fixés dans le code et versionnés. Les propriétés communes activent l'optimisation à l'écriture, le Change Data Feed et le V-Order.

# CELL ********************

TABLE_PROPERTIES = """
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed'      = 'true',
  'delta.parquet.vorder.enabled'    = 'true'
)"""

for schema_name, schema_comment in [
    ("bronze", "Raw data exactly as received from the sources (append-only)."),
    ("silver", "Cleaned, typed and de-duplicated data, one row per business key."),
    ("ops", "Operational metadata: run log and data-quality results."),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name} COMMENT '{schema_comment}'")

spark.sql("SHOW SCHEMAS").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze
# 
# Une table par source. Chaque ligne garde le chemin du fichier JSON d'origine (`source_file`) et l'identifiant d'exécution du pipeline (`pipeline_run_id`).

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.day_ahead_price_raw (
  zone_code            STRING    COMMENT 'Bidding zone (Energy-Charts bzn code)',
  interval_start_utc   TIMESTAMP COMMENT 'Start of the delivery interval, UTC',
  price_eur_mwh        DOUBLE    COMMENT 'Day-ahead clearing price as published',
  unit                 STRING    COMMENT 'Unit returned by the API',
  source_file          STRING    COMMENT 'OneLake path of the landed raw JSON payload',
  ingested_at_utc      TIMESTAMP COMMENT 'When this row was ingested',
  pipeline_run_id      STRING    COMMENT 'Fabric pipeline run id (lineage)'
) USING DELTA
COMMENT 'EPEX Spot day-ahead prices as received from api.energy-charts.info'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.power_generation_raw (
  country_code         STRING,
  interval_start_utc   TIMESTAMP,
  production_type      STRING,
  power_value          DOUBLE   COMMENT 'MW for generation/load, % for share series',
  source_file          STRING,
  ingested_at_utc      TIMESTAMP,
  pipeline_run_id      STRING
) USING DELTA
COMMENT 'Public net electricity generation per production type (Energy-Charts /public_power)'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.weather_hourly_raw (
  zone_code               STRING,
  interval_start_utc      TIMESTAMP,
  temperature_2m_c        DOUBLE,
  cloud_cover_pct         DOUBLE,
  wind_speed_100m_kmh     DOUBLE,
  shortwave_radiation_wm2 DOUBLE,
  source_file             STRING,
  ingested_at_utc         TIMESTAMP,
  pipeline_run_id         STRING
) USING DELTA
COMMENT 'Hourly weather (Open-Meteo) at a representative point of each bidding zone'
{TABLE_PROPERTIES}
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver
# 
# Tables conformes, alimentées par `MERGE` depuis le bronze, ainsi que les tables de référence (zones, calendrier, jours fériés) chargées par `nb_01_load_reference_data`.

# CELL ********************

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.day_ahead_price (
  zone_code              STRING    NOT NULL,
  interval_start_utc     TIMESTAMP NOT NULL,
  interval_end_utc       TIMESTAMP NOT NULL,
  resolution_minutes     INT       COMMENT '15 since the EPEX 15-min MTU go-live (2025-10-01), 60 before',
  interval_start_local   TIMESTAMP COMMENT 'Wall-clock time in the market time zone',
  delivery_date_local    DATE      COMMENT 'Delivery day in the market time zone',
  price_eur_mwh          DOUBLE,
  price_ct_kwh           DOUBLE    COMMENT 'price_eur_mwh / 10',
  updated_at_utc         TIMESTAMP,
  pipeline_run_id        STRING
) USING DELTA
COMMENT 'Day-ahead price per bidding zone and delivery interval (business key: zone_code + interval_start_utc)'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.power_generation (
  country_code         STRING    NOT NULL,
  interval_start_utc   TIMESTAMP NOT NULL,
  production_type      STRING    NOT NULL,
  production_category  STRING    COMMENT 'Renewable | Fossil | Nuclear | Storage | Other | Load | Indicator',
  power_value          DOUBLE,
  value_unit           STRING    COMMENT 'MW or %',
  updated_at_utc       TIMESTAMP,
  pipeline_run_id      STRING
) USING DELTA
COMMENT 'Net public power per country, interval and production type'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.weather_hourly (
  zone_code               STRING    NOT NULL,
  interval_start_utc      TIMESTAMP NOT NULL,
  temperature_2m_c        DOUBLE,
  cloud_cover_pct         DOUBLE,
  wind_speed_100m_kmh     DOUBLE,
  shortwave_radiation_wm2 DOUBLE,
  is_forecast             BOOLEAN   COMMENT 'True when the hour was in the future at ingestion time',
  updated_at_utc          TIMESTAMP,
  pipeline_run_id         STRING
) USING DELTA
COMMENT 'Hourly weather per bidding zone (observed + forecast)'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_bidding_zone (
  zone_code             STRING NOT NULL,
  country_name          STRING,
  country_iso2          STRING,
  energy_charts_country STRING,
  region_name           STRING,
  market_timezone       STRING,
  latitude              DOUBLE,
  longitude             DOUBLE
) USING DELTA
COMMENT 'Reference list of the bidding zones followed by the project'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.dim_date (
  calendar_date      DATE NOT NULL,
  date_key           INT  COMMENT 'yyyyMMdd',
  year_number        INT,
  quarter_number     INT,
  month_number       INT,
  month_name         STRING,
  month_short_name   STRING,
  iso_week_number    INT,
  day_of_month       INT,
  day_of_week_number INT  COMMENT '1 = Monday ... 7 = Sunday (ISO)',
  day_name           STRING,
  is_weekend         BOOLEAN,
  year_month         STRING COMMENT 'yyyy-MM'
) USING DELTA
COMMENT 'Calendar dimension'
{TABLE_PROPERTIES}
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.public_holiday (
  calendar_date  DATE   NOT NULL,
  country_iso2   STRING NOT NULL,
  holiday_name   STRING
) USING DELTA
COMMENT 'Public holidays per country (python-holidays library)'
{TABLE_PROPERTIES}
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ops
# 
# `ops.ingestion_log` reçoit une ligne par notebook et par entité chargée. `ops.data_quality_result` historise les contrôles exécutés par `nb_20_data_quality`.

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.ingestion_log (
  pipeline_run_id STRING, notebook_name STRING, entity_name STRING, zone_code STRING,
  status STRING, rows_read BIGINT, rows_inserted BIGINT, rows_updated BIGINT,
  started_at_utc TIMESTAMP, ended_at_utc TIMESTAMP, message STRING
) USING DELTA COMMENT 'One line per notebook execution and entity'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.data_quality_result (
  check_run_at_utc TIMESTAMP, pipeline_run_id STRING, check_name STRING, table_name STRING,
  zone_code STRING, check_date DATE, severity STRING, passed BOOLEAN,
  observed_value DOUBLE, expected_value DOUBLE, details STRING
) USING DELTA COMMENT 'Data-quality checks executed after each load'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW TABLES IN bronze")
        .unionByName(spark.sql("SHOW TABLES IN silver"))
        .unionByName(spark.sql("SHOW TABLES IN ops")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
