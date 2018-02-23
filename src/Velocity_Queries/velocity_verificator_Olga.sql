WITH regions_spotify AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-swift.swift_alerts.trending_tracks_regions`
WHERE _PARTITIONTIME = "2017-12-14 00:00:00" and country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
olga_regions_spotify AS (
SELECT rank_lw, streams_lw, count(1) as countt
--FROM `umg-swift.swift_alerts.trending_tracks_regions`
FROM `umg-user.Olga.velocities_20171214_regions`
WHERE
country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
countries_spotify AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-swift.swift_alerts.trending_tracks_countries`
WHERE _PARTITIONTIME = "2017-12-14 00:00:00" and country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
olga_countries_spotify AS (
SELECT rank_lw, streams_lw, count(1) as countt
--FROM `umg-swift.swift_alerts.trending_tracks_regions`
FROM `umg-user.Olga.velocities_20171214_countries`
WHERE
country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),

regions_apple AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-swift.swift_alerts.trending_tracks_apple_regions`
WHERE _PARTITIONTIME = "2017-12-01 00:00:00" and country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
olga_regions_apple AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-user.Olga.velocities_AM_20171220_regions`
WHERE
country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
countries_apple AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-swift.swift_alerts.trending_tracks_apple_countries`
WHERE _PARTITIONTIME = "2017-12-01 00:00:00" and country_code = 'US' --and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
olga_countries_apple AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-user.Olga.velocities_AM_20171220_countries`
WHERE
country_code = 'US' --and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
)
/*
,
base AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-swift.swift_alerts.velocity_base_table_v2_apple`
WHERE _PARTITIONTIME = "2017-12-01 00:00:00" and country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
),
olgabase AS (
SELECT rank_lw, streams_lw, count(1) as countt
FROM `umg-user.Olga.velocities_AM_20171220`
WHERE
country_code = 'US' and region_dma_code = '803'
group by rank_lw, streams_lw
having countt > 1
order by streams_lw
)*/

SELECT * FROM olgacountries