CREATE TABLE [gold].[dim_date] (
    [date_key]           INT          NOT NULL,
    [calendar_date]      DATE         NOT NULL,
    [year_number]        SMALLINT     NULL,
    [quarter_number]     SMALLINT     NULL,
    [month_number]       SMALLINT     NULL,
    [month_name]         VARCHAR (12) NULL,
    [month_short_name]   VARCHAR (4)  NULL,
    [year_month]         CHAR (7)     NULL,
    [iso_week_number]    SMALLINT     NULL,
    [day_of_month]       SMALLINT     NULL,
    [day_of_week_number] SMALLINT     NULL,
    [day_name]           VARCHAR (12) NULL,
    [is_weekend]         BIT          NULL
);


GO