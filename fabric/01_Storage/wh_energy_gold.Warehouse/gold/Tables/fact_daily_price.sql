CREATE TABLE [gold].[fact_daily_price] (
    [zone_code]                VARCHAR (16)    NOT NULL,
    [date_key]                 INT             NOT NULL,
    [avg_price_ct_kwh]         DECIMAL (10, 4) NULL,
    [min_price_ct_kwh]         DECIMAL (10, 4) NULL,
    [max_price_ct_kwh]         DECIMAL (10, 4) NULL,
    [stdev_price_ct_kwh]       DECIMAL (10, 4) NULL,
    [peak_avg_price_ct_kwh]    DECIMAL (10, 4) NULL,
    [offpeak_avg_price_ct_kwh] DECIMAL (10, 4) NULL,
    [negative_price_intervals] SMALLINT        NULL,
    [intervals_in_day]         SMALLINT        NULL,
    [is_public_holiday]        BIT             NOT NULL,
    [holiday_name]             VARCHAR (100)   NULL
);


GO