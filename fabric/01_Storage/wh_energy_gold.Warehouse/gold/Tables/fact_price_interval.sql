CREATE TABLE [gold].[fact_price_interval] (
    [zone_code]            VARCHAR (16)    NOT NULL,
    [date_key]             INT             NOT NULL,
    [time_key]             SMALLINT        NOT NULL,
    [interval_start_utc]   DATETIME2 (0)   NOT NULL,
    [interval_start_local] DATETIME2 (0)   NOT NULL,
    [resolution_minutes]   SMALLINT        NOT NULL,
    [price_eur_mwh]        DECIMAL (10, 3) NULL,
    [price_ct_kwh]         DECIMAL (10, 4) NULL,
    [is_negative_price]    BIT             NOT NULL,
    [price_rank_in_day]    SMALLINT        NOT NULL,
    [intervals_in_day]     SMALLINT        NOT NULL,
    [consumption_advice]   VARCHAR (40)    NOT NULL
);


GO