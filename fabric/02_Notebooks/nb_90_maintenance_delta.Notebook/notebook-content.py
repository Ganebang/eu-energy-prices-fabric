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

# # nb_90_maintenance_delta
# **Rôle** : maintenance hebdomadaire des tables Delta du Lakehouse.
# * `OPTIMIZE ... VORDER` : compacte les petits fichiers produits par les `MERGE` quotidiens et applique
#   l'ordonnancement V-Order (lecture plus rapide pour le SQL endpoint et Power BI Direct Lake) ;
# * `VACUUM` : supprime les fichiers Parquet qui ne sont plus référencés au-delà de la rétention (7 jours par défaut,
#   ce qui préserve le *time travel* d'une semaine).

# PARAMETERS CELL ********************

retention_hours = 168
schemas_to_maintain = "bronze,silver,ops"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

maintenance_report = []
for schema_name in [s.strip() for s in schemas_to_maintain.split(",") if s.strip()]:
    for table_row in spark.sql(f"SHOW TABLES IN {schema_name}").collect():
        full_table_name = f"{schema_name}.{table_row.tableName}"
        started = datetime.utcnow()
        spark.sql(f"OPTIMIZE {full_table_name} VORDER")
        spark.sql(f"VACUUM {full_table_name} RETAIN {int(retention_hours)} HOURS")
        file_count = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]["numFiles"]
        maintenance_report.append((full_table_name, file_count, (datetime.utcnow() - started).seconds))
        print(f"{full_table_name:45s} files={file_count:5d}  {maintenance_report[-1][2]}s")

display(spark.createDataFrame(maintenance_report, "table_name STRING, files_after BIGINT, duration_seconds INT"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
