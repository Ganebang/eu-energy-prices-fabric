/* =====================================================================
   gold.usp_refresh_gold : chargement de la couche GOLD
   - lecture inter-bases (3 parties) depuis le SQL endpoint du Lakehouse :
       [lh_energy].[silver].[table]  -- même workspace, sans copie ;
   - fenêtre glissante (@lookback_days) : DELETE + INSERT idempotent ;
   - transaction : Power BI ne voit jamais un état intermédiaire.
   Appelée par le pipeline pl_energy_daily (activité "Stored procedure").
   NB collation : le SQL endpoint du Lakehouse est en Latin1_General_100_BIN2_UTF8
   (sensible à la casse), le Warehouse en CI : les jointures inter-bases
   sur du texte précisent donc COLLATE explicitement.
   ===================================================================== */
CREATE   PROCEDURE gold.usp_refresh_gold
    @lookback_days   INT          = 3,
    @full_reload     BIT          = 0,
    @pipeline_run_id VARCHAR(64)  = 'manual'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @from_date DATE =
        CASE WHEN @full_reload = 1 THEN '2000-01-01'
             ELSE DATEADD(DAY, -@lookback_days, CAST(SYSUTCDATETIME() AS DATE)) END;
    DECLARE @from_key INT = YEAR(@from_date) * 10000 + MONTH(@from_date) * 100 + DAY(@from_date);

    /* ---------- 1. Dimensions : rafraîchissement complet (petites tables) ---------- */
    BEGIN TRANSACTION;

    DELETE FROM gold.dim_bidding_zone;
    INSERT INTO gold.dim_bidding_zone
    SELECT zone_code, country_name, country_iso2, region_name, market_timezone, latitude, longitude
    FROM   lh_energy.silver.dim_bidding_zone;

    DELETE FROM gold.dim_date;
    INSERT INTO gold.dim_date
    SELECT date_key, calendar_date, year_number, quarter_number, month_number, month_name,
           month_short_name, year_month, iso_week_number, day_of_month, day_of_week_number,
           day_name, is_weekend
    FROM   lh_energy.silver.dim_date;

    COMMIT TRANSACTION;

    /* ---------- 2. Faits : fenêtre glissante ---------- */
    BEGIN TRANSACTION;

    /* 2a. Prix par intervalle (15 min) + rang dans la journée + conseil */
    DELETE FROM gold.fact_price_interval WHERE date_key >= @from_key;

    WITH ranked AS (
        SELECT  p.zone_code,
                YEAR(p.delivery_date_local) * 10000 + MONTH(p.delivery_date_local) * 100 + DAY(p.delivery_date_local) AS date_key,
                DATEPART(HOUR, p.interval_start_local) * 60 + DATEPART(MINUTE, p.interval_start_local)                AS time_key,
                p.interval_start_utc,
                p.interval_start_local,
                p.resolution_minutes,
                p.price_eur_mwh,
                p.price_ct_kwh,
                ROW_NUMBER() OVER (PARTITION BY p.zone_code, p.delivery_date_local ORDER BY p.price_eur_mwh, p.interval_start_utc) AS price_rank_in_day,
                COUNT(*)     OVER (PARTITION BY p.zone_code, p.delivery_date_local)                                    AS intervals_in_day
        FROM    lh_energy.silver.day_ahead_price AS p
        WHERE   p.delivery_date_local >= @from_date
    )
    INSERT INTO gold.fact_price_interval
    SELECT  zone_code, date_key, time_key, interval_start_utc, interval_start_local, resolution_minutes,
            price_eur_mwh, price_ct_kwh,
            CASE WHEN price_eur_mwh < 0 THEN 1 ELSE 0 END,
            price_rank_in_day, intervals_in_day,
            CASE
                WHEN price_eur_mwh < 0                                          THEN '1-Charger + consommer (prix négatif)'
                WHEN price_rank_in_day <= CEILING(intervals_in_day * 0.25)      THEN '2-Charger la batterie'
                WHEN price_rank_in_day <= CEILING(intervals_in_day * 0.60)      THEN '3-Consommer sur le réseau'
                WHEN price_rank_in_day <= CEILING(intervals_in_day * 0.85)      THEN '4-Utiliser la batterie'
                ELSE                                                                 '5-Batterie + revente'
            END
    FROM    ranked;

    /* 2b. Statistiques journalières + jours fériés */
    DELETE FROM gold.fact_daily_price WHERE date_key >= @from_key;

    INSERT INTO gold.fact_daily_price
    SELECT  f.zone_code,
            f.date_key,
            AVG(f.price_ct_kwh),
            MIN(f.price_ct_kwh),
            MAX(f.price_ct_kwh),
            CAST(STDEV(f.price_ct_kwh) AS DECIMAL(10,4)),
            AVG(CASE WHEN t.is_peak_period = 1 THEN f.price_ct_kwh END),
            AVG(CASE WHEN t.is_peak_period = 0 THEN f.price_ct_kwh END),
            SUM(CAST(f.is_negative_price AS SMALLINT)),
            COUNT(*),
            CASE WHEN MAX(h.holiday_name) IS NULL THEN 0 ELSE 1 END,
            MAX(h.holiday_name)
    FROM    gold.fact_price_interval AS f
    JOIN    gold.dim_time_of_day     AS t ON t.time_key = f.time_key
    JOIN    gold.dim_bidding_zone    AS z ON z.zone_code = f.zone_code
    JOIN    gold.dim_date            AS d ON d.date_key = f.date_key
    LEFT JOIN lh_energy.silver.public_holiday AS h
           ON h.country_iso2 COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8 = z.country_iso2 AND h.calendar_date = d.calendar_date
    WHERE   f.date_key >= @from_key
    GROUP BY f.zone_code, f.date_key;

    /* 2c. Fait horaire : prix x production x météo (jointure sur l'heure UTC) */
    DELETE FROM gold.fact_hourly_energy WHERE date_key >= @from_key;

    WITH hourly_price AS (
        SELECT  zone_code,
                DATEADD(HOUR, DATEDIFF(HOUR, CAST('2000-01-01' AS DATETIME2(0)), interval_start_utc), CAST('2000-01-01' AS DATETIME2(0)))   AS hour_start_utc,
                MIN(DATEADD(HOUR, DATEDIFF(HOUR, CAST('2000-01-01' AS DATETIME2(0)), interval_start_local), CAST('2000-01-01' AS DATETIME2(0)))) AS hour_start_local,
                MIN(date_key)                                                                                  AS date_key,
                AVG(price_ct_kwh)                                                                              AS avg_price_ct_kwh
        FROM    gold.fact_price_interval
        WHERE   date_key >= @from_key
        GROUP BY zone_code,
                 DATEADD(HOUR, DATEDIFF(HOUR, CAST('2000-01-01' AS DATETIME2(0)), interval_start_utc), CAST('2000-01-01' AS DATETIME2(0)))
    ),
    hourly_generation AS (
        SELECT  g.country_code,
                DATEADD(HOUR, DATEDIFF(HOUR, CAST('2000-01-01' AS DATETIME2(0)), g.interval_start_utc), CAST('2000-01-01' AS DATETIME2(0))) AS hour_start_utc,
                AVG(CASE WHEN g.production_type = 'Solar'                   THEN g.power_value END) AS solar_mw,
                AVG(CASE WHEN g.production_type = 'Wind onshore'            THEN g.power_value END) AS wind_onshore_mw,
                AVG(CASE WHEN g.production_type = 'Wind offshore'           THEN g.power_value END) AS wind_offshore_mw,
                AVG(CASE WHEN g.production_type = 'Nuclear'                 THEN g.power_value END) AS nuclear_mw,
                AVG(CASE WHEN g.production_type = 'Fossil gas'              THEN g.power_value END) AS fossil_gas_mw,
                AVG(CASE WHEN g.production_type = 'Load'                    THEN g.power_value END) AS load_mw,
                AVG(CASE WHEN g.production_type = 'Renewable share of load' THEN g.power_value END) AS renewable_share_pct
        FROM    lh_energy.silver.power_generation AS g
        WHERE   g.interval_start_utc >= DATEADD(DAY, -1, CAST(@from_date AS DATETIME2(0)))
        GROUP BY g.country_code,
                 DATEADD(HOUR, DATEDIFF(HOUR, CAST('2000-01-01' AS DATETIME2(0)), g.interval_start_utc), CAST('2000-01-01' AS DATETIME2(0)))
    )
    INSERT INTO gold.fact_hourly_energy
    SELECT  p.zone_code, p.date_key, DATEPART(HOUR, p.hour_start_local), p.hour_start_utc, p.hour_start_local,
            p.avg_price_ct_kwh,
            g.solar_mw, g.wind_onshore_mw, g.wind_offshore_mw, g.nuclear_mw, g.fossil_gas_mw, g.load_mw,
            g.renewable_share_pct,
            w.temperature_2m_c, w.wind_speed_100m_kmh, w.shortwave_radiation_wm2, w.cloud_cover_pct,
            w.is_forecast
    FROM    hourly_price AS p
    JOIN    gold.dim_bidding_zone AS z ON z.zone_code = p.zone_code
    LEFT JOIN hourly_generation   AS g ON UPPER(g.country_code) COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8 = z.country_iso2
                                      AND g.hour_start_utc = p.hour_start_utc
    LEFT JOIN lh_energy.silver.weather_hourly AS w
           ON w.zone_code COLLATE Latin1_General_100_CI_AS_KS_WS_SC_UTF8 = p.zone_code AND w.interval_start_utc = p.hour_start_utc;

    /* 2d. Simulation d'arbitrage d'une batterie domestique (1 cycle / jour)
           Charge pendant les N heures les moins chères, décharge pendant les N plus chères,
           N = capacité / puissance. Borne haute indicative (prix de gros, hors taxes et TURPE). */
    DELETE FROM gold.fact_daily_battery WHERE date_key >= @from_key;

    WITH hour_rank AS (
        SELECT  h.zone_code, h.date_key, h.avg_price_ct_kwh,
                ROW_NUMBER() OVER (PARTITION BY h.zone_code, h.date_key ORDER BY h.avg_price_ct_kwh ASC)  AS cheap_rank,
                ROW_NUMBER() OVER (PARTITION BY h.zone_code, h.date_key ORDER BY h.avg_price_ct_kwh DESC) AS expensive_rank
        FROM    gold.fact_hourly_energy AS h
        WHERE   h.date_key >= @from_key AND h.avg_price_ct_kwh IS NOT NULL
    ),
    per_scenario AS (
        SELECT  r.zone_code, r.date_key, s.scenario_key, s.capacity_kwh, s.roundtrip_efficiency,
                CAST(CEILING(s.capacity_kwh / s.power_kw) AS SMALLINT)                                               AS cycle_hours,
                AVG(CASE WHEN r.cheap_rank     <= CEILING(s.capacity_kwh / s.power_kw) THEN r.avg_price_ct_kwh END) AS avg_charge_price_ct_kwh,
                AVG(CASE WHEN r.expensive_rank <= CEILING(s.capacity_kwh / s.power_kw) THEN r.avg_price_ct_kwh END) AS avg_discharge_price_ct_kwh
        FROM    hour_rank AS r
        CROSS JOIN gold.battery_scenario AS s
        GROUP BY r.zone_code, r.date_key, s.scenario_key, s.capacity_kwh, s.power_kw, s.roundtrip_efficiency
    )
    INSERT INTO gold.fact_daily_battery
    SELECT  zone_code, date_key, scenario_key, cycle_hours,
            avg_charge_price_ct_kwh, avg_discharge_price_ct_kwh,
            capacity_kwh,
            capacity_kwh * roundtrip_efficiency,
            capacity_kwh * avg_charge_price_ct_kwh / 100.0,
            capacity_kwh * roundtrip_efficiency * avg_discharge_price_ct_kwh / 100.0,
            CASE WHEN capacity_kwh * (roundtrip_efficiency * avg_discharge_price_ct_kwh - avg_charge_price_ct_kwh) > 0
                 THEN capacity_kwh * (roundtrip_efficiency * avg_discharge_price_ct_kwh - avg_charge_price_ct_kwh) / 100.0
                 ELSE 0 END
    FROM    per_scenario;

    COMMIT TRANSACTION;

    SELECT  @pipeline_run_id AS pipeline_run_id, @from_date AS refreshed_from_date,
            (SELECT COUNT(*) FROM gold.fact_price_interval WHERE date_key >= @from_key) AS price_intervals,
            (SELECT COUNT(*) FROM gold.fact_hourly_energy  WHERE date_key >= @from_key) AS hourly_rows,
            (SELECT COUNT(*) FROM gold.fact_daily_battery  WHERE date_key >= @from_key) AS battery_rows;
END;

GO