CREATE TABLE [gold].[fact_hourly_energy] (
    [zone_code]               VARCHAR (16)    NOT NULL,
    [date_key]                INT             NOT NULL,
    [hour_of_day]             SMALLINT        NOT NULL,
    [hour_start_utc]          DATETIME2 (0)   NOT NULL,
    [hour_start_local]        DATETIME2 (0)   NOT NULL,
    [avg_price_ct_kwh]        DECIMAL (10, 4) NULL,
    [solar_mw]                DECIMAL (12, 1) NULL,
    [wind_onshore_mw]         DECIMAL (12, 1) NULL,
    [wind_offshore_mw]        DECIMAL (12, 1) NULL,
    [nuclear_mw]              DECIMAL (12, 1) NULL,
    [fossil_gas_mw]           DECIMAL (12, 1) NULL,
    [load_mw]                 DECIMAL (12, 1) NULL,
    [renewable_share_pct]     DECIMAL (6, 2)  NULL,
    [temperature_c]           DECIMAL (6, 2)  NULL,
    [wind_speed_100m_kmh]     DECIMAL (6, 2)  NULL,
    [shortwave_radiation_wm2] DECIMAL (8, 2)  NULL,
    [cloud_cover_pct]         DECIMAL (6, 2)  NULL,
    [is_weather_forecast]     BIT             NULL
);


GO