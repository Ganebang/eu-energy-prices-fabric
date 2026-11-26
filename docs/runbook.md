# Exploitation

## Exécution planifiée

`pl_energy_daily` tourne toutes les trois heures (fuseau Europe de l'Ouest). Avec les paramètres par défaut, chaque exécution :

- recharge les prix de J-1 à J+1 et la production de J-2 à aujourd'hui pour les zones de `p_zone_codes` ;
- recharge la météo des trois derniers jours et les prévisions des trois prochains ;
- contrôle les trois derniers jours ;
- reconstruit la couche gold sur les trois derniers jours (`p_lookback_days`).

Les prix du lendemain sont publiés vers 12h45 ; ils apparaissent dans le rapport après l'exécution suivante.

## Suivi

```sql
-- Dernières exécutions des notebooks
SELECT TOP 50 pipeline_run_id, notebook_name, zone_code, status,
       rows_read, rows_inserted, rows_updated, started_at_utc, ended_at_utc, message
FROM   lh_energy.ops.ingestion_log
ORDER  BY started_at_utc DESC;

-- Contrôles en échec sur 7 jours
SELECT check_run_at_utc, check_name, zone_code, check_date, severity, observed_value, expected_value, details
FROM   lh_energy.ops.data_quality_result
WHERE  passed = 0 AND check_run_at_utc >= DATEADD(DAY, -7, SYSUTCDATETIME())
ORDER  BY check_run_at_utc DESC;
```

## Recharger une période

Lancer `pl_energy_daily` manuellement avec :

| Paramètre | Valeur |
|---|---|
| `p_zone_codes` | zones concernées, par exemple `FR,BE` |
| `p_start_date`, `p_end_date` | période au format `yyyy-MM-dd` |
| `p_lookback_days` | nombre de jours entre `p_start_date` et aujourd'hui, pour que la couche gold couvre la période |

Les appels API sont découpés automatiquement (31 jours pour les prix, 14 jours pour la production). Les `MERGE` rendent l'opération sans risque de doublon.

Pour reconstruire toute la couche gold sans rappeler les API : `EXEC gold.usp_refresh_gold @full_reload = 1;` sur le warehouse.

## Incidents courants

| Symptôme | Cause probable | Action |
|---|---|---|
| `nb_20_data_quality` en échec sur `price_completeness` | journée incomplète côté source, souvent J+1 avant publication complète | vérifier `ops.data_quality_result`, relancer le pipeline après 13h00 |
| `price_within_market_limits` en échec | prix hors bornes SDAC, ou changement d'unité dans l'API | comparer avec le fichier brut indiqué dans `bronze.day_ahead_price_raw.source_file` |
| `NO_DATA` dans `ops.ingestion_log` | API sans données pour la fenêtre | normal pour J+1 le matin ; sinon tester l'URL de l'API |
| HTTP 429 dans les logs | limite de l'API Energy-Charts | le client réessaie avec un délai croissant ; espacer les rechargements longs |
| Rapport vide pour les dernières heures | la procédure gold a lu une version ancienne du silver | vérifier que la cellule `refreshMetadata` de `nb_20` s'est exécutée |

## Maintenance

`nb_90_maintenance_delta`, une fois par semaine : `OPTIMIZE ... VORDER` puis `VACUUM` avec 7 jours de rétention.
