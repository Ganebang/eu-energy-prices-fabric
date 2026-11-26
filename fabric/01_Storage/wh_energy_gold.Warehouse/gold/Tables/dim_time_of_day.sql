CREATE TABLE [gold].[dim_time_of_day] (
    [time_key]        SMALLINT     NOT NULL,
    [hour_of_day]     SMALLINT     NOT NULL,
    [quarter_of_hour] SMALLINT     NOT NULL,
    [time_label]      CHAR (5)     NOT NULL,
    [hour_label]      CHAR (5)     NOT NULL,
    [day_period]      VARCHAR (20) NOT NULL,
    [is_peak_period]  BIT          NOT NULL
);


GO