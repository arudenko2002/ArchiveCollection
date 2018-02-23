WITH  getToday AS (
    --SELECT CURRENT_DATE()
    SELECT DATE("{ExecutionDate}") as today
),
-- writes umg-dev.swift_trends.isrc_first_stream_date
get_release_dates AS(
    SELECT * FROM isrc, MIN(stream_date) as first_stream_date
        from `umg-partner.spotify.daily_track_history`
        group by isrc
),
-- writes to umg-dev.swift_trends.track_archive_by_stream_date\$date
set_daily_history AS (
    SELECT
    s.isrc as isrc,
    fsd.first_stream_date as first_stream_date,
    s.stream_date as stream_date,
    date_diff(s.stream_date, fsd.first_stream_date, DAY) as day_since_first_stream,
    s.user_country_code as user_country_code,
    s.user_country_name as user_country_name,
    s.stream_source as stream_source,
    count(user_id) as total_stream_count,
    count(case when engagement_style = 'Lean Back' then 1 end) as lean_back_stream_count,
    count(case when engagement_style = 'Lean Forward' then 1 end) as lean_forward_stream_count,
    current_timestamp() as load_datetime
    FROM (
        SELECT * FROM `umg-partner.spotify.streams`
        WHERE _PARTITIONTIME = TIMESTAMP({DATE})
      ) s
    INNER JOIN (
        SELECT * FROM isrc, MIN(stream_date) as first_stream_date
            from `umg-partner.spotify.daily_track_history`
            group by isrc
    ) fsd ON s.isrc = fsd.isrc
    WHERE date_diff(s.stream_date, fsd.first_stream_date, DAY) <= 365
    GROUP BY
    isrc,
    first_stream_date,
    stream_date,
    day_since_first_stream,
    user_country_code,
    user_country_name,
    stream_source
),
-- writes to umg-dev.swift_trends.track_archive\$1xxx-01-01
get_daily_history AS (
    SELECT *
    FROM `umg-dev.swift_trends.track_archive_by_stream_date`
    WHERE day_since_first_stream = CAST({DaySinceFirstStream} AS int64)
)

select * from set_daily_history limit 2000