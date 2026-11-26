CREATE TABLE [gold].[dim_bidding_zone] (
    [zone_code]       VARCHAR (16) NOT NULL,
    [country_name]    VARCHAR (64) NULL,
    [country_iso2]    CHAR (2)     NULL,
    [region_name]     VARCHAR (32) NULL,
    [market_timezone] VARCHAR (40) NULL,
    [latitude]        FLOAT (53)   NULL,
    [longitude]       FLOAT (53)   NULL
);


GO