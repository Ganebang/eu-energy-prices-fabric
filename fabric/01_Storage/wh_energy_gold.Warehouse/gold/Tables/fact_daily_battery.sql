CREATE TABLE [gold].[fact_daily_battery] (
    [zone_code]                  VARCHAR (16)    NOT NULL,
    [date_key]                   INT             NOT NULL,
    [scenario_key]               SMALLINT        NOT NULL,
    [cycle_hours]                SMALLINT        NOT NULL,
    [avg_charge_price_ct_kwh]    DECIMAL (10, 4) NULL,
    [avg_discharge_price_ct_kwh] DECIMAL (10, 4) NULL,
    [energy_charged_kwh]         DECIMAL (8, 2)  NULL,
    [energy_delivered_kwh]       DECIMAL (8, 2)  NULL,
    [charge_cost_eur]            DECIMAL (10, 4) NULL,
    [discharge_value_eur]        DECIMAL (10, 4) NULL,
    [arbitrage_gain_eur]         DECIMAL (10, 4) NULL
);


GO