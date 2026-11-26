CREATE TABLE [gold].[battery_scenario] (
    [scenario_key]         SMALLINT       NOT NULL,
    [scenario_name]        VARCHAR (40)   NOT NULL,
    [capacity_kwh]         DECIMAL (6, 2) NOT NULL,
    [power_kw]             DECIMAL (6, 2) NOT NULL,
    [roundtrip_efficiency] DECIMAL (4, 3) NOT NULL
);


GO