WITH velocity_countries AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_countries`
WHERE _PARTITIONTIME >= "2018-01-31 00:00:00"
group by 1
order by pt
),
velocity_regions AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_regions`
WHERE _PARTITIONTIME >= "2018-01-31 00:00:00"
group by 1
order by pt
),
velocity_regions_apple AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_regions`
WHERE _PARTITIONTIME >= "2018-01-31 00:00:00"
group by 1
order by pt
),
velocity_countries_apple AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_countries`
WHERE _PARTITIONTIME >= "2018-01-31 00:00:00"
group by 1
order by pt
)

SELECT * FROM velocity_countries_apple