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

# # nb_01_load_reference_data
# 
# Chargement des tables de référence de la couche silver.
# 
# | | |
# |---|---|
# | Couche | Silver |
# | Écrit | `silver.dim_bidding_zone`, `silver.dim_date`, `silver.public_holiday` |
# | Sources | module `energy_toolkit.reference`, bibliothèque `holidays` |
# | Déclenchement | manuel, après `nb_00_setup_lakehouse` puis à chaque ajout de zone |
# | Ré-exécutable | oui, rechargement complet (`overwrite`) |
# 
# Paramètres :
# - `calendar_start_date`, `calendar_end_date` : bornes du calendrier généré (`yyyy-MM-dd`).
# 
# Les jours fériés sont chargés parce qu'ils modifient la demande et donc le profil de prix ; ils sont repris dans `gold.fact_daily_price`.

# PARAMETERS CELL ********************

calendar_start_date = "2024-01-01"
calendar_end_date = "2027-12-31"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date

import holidays
from pyspark.sql import functions as F

from energy_toolkit import __version__ as toolkit_version
from energy_toolkit.reference import zones_as_rows

spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"energy_toolkit {toolkit_version} | holidays {holidays.__version__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Zones de marché
# 
# La liste des zones est définie dans `energy_toolkit.reference`, versionnée avec le code. La table est petite : elle est réécrite à chaque exécution.

# CELL ********************

bidding_zone_df = spark.createDataFrame(zones_as_rows())
(bidding_zone_df
 .select("zone_code", "country_name", "country_iso2", "energy_charts_country",
         "region_name", "market_timezone", "latitude", "longitude")
 .write.mode("overwrite").option("overwriteSchema", "false")
 .saveAsTable("silver.dim_bidding_zone"))

display(spark.table("silver.dim_bidding_zone").orderBy("region_name", "zone_code"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Calendrier
# 
# Le calendrier est produit avec `sequence()` et `explode()`, une ligne par jour. `day_of_week_number` suit la norme ISO (1 = lundi).

# CELL ********************

calendar_df = (
    spark.sql(f"SELECT explode(sequence(to_date('{calendar_start_date}'), to_date('{calendar_end_date}'), interval 1 day)) AS calendar_date")
    .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year_number", F.year("calendar_date"))
    .withColumn("quarter_number", F.quarter("calendar_date"))
    .withColumn("month_number", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("calendar_date", "MMM"))
    .withColumn("iso_week_number", F.weekofyear("calendar_date"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date"))
    .withColumn("day_of_week_number", ((F.dayofweek("calendar_date") + 5) % 7 + 1).cast("int"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("is_weekend", F.col("day_of_week_number") >= 6)
    .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
)
calendar_df.write.mode("overwrite").saveAsTable("silver.dim_date")
print(f"silver.dim_date: {calendar_df.count()} days")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Jours fériés
# 
# Quelques centaines de lignes : la liste est construite côté driver puis convertie en DataFrame avec un schéma explicite.

# CELL ********************

country_codes = sorted({row["country_iso2"] for row in zones_as_rows()})
year_range = range(int(calendar_start_date[:4]), int(calendar_end_date[:4]) + 1)

holiday_rows = [
    (holiday_date, country_code, holiday_name)
    for country_code in country_codes
    for holiday_date, holiday_name in holidays.country_holidays(country_code, years=year_range).items()
]
public_holiday_df = spark.createDataFrame(
    holiday_rows, "calendar_date DATE, country_iso2 STRING, holiday_name STRING"
).dropDuplicates(["calendar_date", "country_iso2"])

public_holiday_df.write.mode("overwrite").saveAsTable("silver.public_holiday")
display(public_holiday_df.groupBy("country_iso2").count().orderBy("country_iso2"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
