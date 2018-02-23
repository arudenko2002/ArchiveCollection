WITH AAA AS (
SELECT "base" as name,count(*)  FROM `umg-swift.swift_alerts.velocity_base_table_v2_apple`
WHERE _PARTITIONTIME = "2017-12-14 00:00:00"
--OR _PARTITIONTIME = "2018-01-06 00:00:00"
UNION ALL
SELECT "regions" as name,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_regions`
WHERE _PARTITIONTIME = "2017-12-14 00:00:00"
--OR _PARTITIONTIME = "2018-01-06 00:00:00"
UNION ALL
SELECT "countries" as name,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_countries`
WHERE _PARTITIONTIME = "2017-12-14 00:00:00"
--OR _PARTITIONTIME = "2018-01-06 00:00:00"
LIMIT 1000
),
BBB AS (
SELECT "base" as name,count(*)  FROM `umg-swift.swift_alerts.velocity_base_table_v2_schemaless`
WHERE
_PARTITIONTIME = "2018-01-23 00:00:00"
--OR
--_PARTITIONTIME = "2018-01-15 00:00:00"
UNION ALL
SELECT "regions" as name,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_regions_schemaless`
WHERE
_PARTITIONTIME = "2018-01-23 00:00:00"
--OR
--_PARTITIONTIME = "2018-01-15 00:00:00"
UNION ALL
SELECT "countries" as name,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_countries_schemaless`
WHERE
_PARTITIONTIME = "2018-01-23 00:00:00"
--OR
--_PARTITIONTIME = "2018-01-15 00:00:00"
LIMIT 1000
),
CCC AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_countries`
WHERE _PARTITIONTIME >= "2017-12-14 00:00:00"
group by 1
order by pt
),
DDD AS (
SELECT _PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.velocity_base_table_v2_schemaless`
WHERE _PARTITIONTIME >= "2018-01-17 00:00:00"
group by 1
order by pt
)

SELECT * FROM CCC