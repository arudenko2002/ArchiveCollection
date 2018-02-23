#Step 1: create a base table
#The query takes around 3 minutes to run and returns 160 - 180 millions of records
#This is the base table to be used in further analysis - trending tracks, artists, countries, regions, etc.
SELECT *
FROM
(SELECT
	   t.report_date AS report_date,
       cn.canopus_id AS canopus_id,
       c.resource_rollup_id AS resource_rollup_id,
	   cn.default_name AS track_artist,
       c.formatted_title  AS track_title,
       t.user_country_code AS country_code,
       t.user_country_name AS country_name,
       t.region_dma_code AS region_dma_code,
       t.dma_name AS dma_name,
       t.isrc AS isrc,
       t.week AS week,
       t.streams AS streams,
       t.streams_collection AS streams_collection,
       t.streams_playlist AS streams_playlist,
       t.streams_undeveloped_playlist AS streams_undeveloped_playlist,
       t.streams_other AS streams_other,
       t.streams_artist AS streams_artist,
       t.streams_album AS streams_album,
       t.streams_search AS streams_search
FROM
(
SELECT *
FROM
    (SELECT
    date("2017-12-14") as report_date,
    user_country_code,
    user_country_name,
    CONCAT(user_dma_number, user_region_code) AS region_dma_code,
    user_dma_name AS dma_name,
    isrc,
    COUNT (1) AS streams,
    COUNT (CASE WHEN stream_source = 'collection' THEN 1 END) AS streams_collection,
    COUNT (CASE WHEN stream_source = 'other' THEN 1 END) AS streams_other,
    COUNT (CASE WHEN stream_source = 'artist' THEN 1 END) AS streams_artist,
    COUNT (CASE WHEN stream_source = 'album' THEN 1 END) AS streams_album,
    COUNT (CASE WHEN stream_source = 'search' THEN 1 END) AS streams_search,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri = '' THEN 1 END) AS streams_undeveloped_playlist,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri != '' THEN 1 END) AS streams_playlist,
    CASE WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -13 day)) and timestamp(date_add(date("2017-12-14"), interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -6 day)) and timestamp(date("2017-12-14")) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
    WHERE _partitiontime between timestamp(date_add(date("2017-12-14"), interval - 13 day)) and timestamp(date("2017-12-14"))
    GROUP BY user_country_code, user_country_name, region_dma_code, dma_name, isrc, week
    )
    UNION ALL
    (SELECT
    date("2017-12-14") as report_date,
    'XX' AS user_country_code,
    'Global' AS user_country_name,
    '' AS region_dma_code,
    '' AS dma_name,
    isrc,
    COUNT (1) AS streams,
    COUNT (CASE WHEN stream_source = 'collection' THEN 1 END) AS streams_collection,
    COUNT (CASE WHEN stream_source = 'other' THEN 1 END) AS streams_other,
    COUNT (CASE WHEN stream_source = 'artist' THEN 1 END) AS streams_artist,
    COUNT (CASE WHEN stream_source = 'album' THEN 1 END) AS streams_album,
    COUNT (CASE WHEN stream_source = 'search' THEN 1 END) AS streams_search,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri = '' THEN 1 END) AS streams_undeveloped_playlist,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri != '' THEN 1 END) AS streams_playlist,
    CASE WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -13 day)) and timestamp(date_add(date("2017-12-14"), interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -6 day)) and timestamp(date("2017-12-14")) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
    WHERE _partitiontime between timestamp(date_add(date("2017-12-14"), interval - 13 day)) and timestamp(date("2017-12-14"))
    GROUP BY user_country_code, user_country_name, region_dma_code, dma_name, isrc, week
    )
    UNION ALL
    (SELECT
    date("2017-12-14") as report_date,
    'EX-US' AS user_country_code,
    'Global Ex-U.S.' AS user_country_name,
    '' AS region_dma_code,
    '' AS dma_name,
    isrc,
    COUNT (1) AS streams,
    COUNT (CASE WHEN stream_source = 'collection' THEN 1 END) AS streams_collection,
    COUNT (CASE WHEN stream_source = 'other' THEN 1 END) AS streams_other,
    COUNT (CASE WHEN stream_source = 'artist' THEN 1 END) AS streams_artist,
    COUNT (CASE WHEN stream_source = 'album' THEN 1 END) AS streams_album,
    COUNT (CASE WHEN stream_source = 'search' THEN 1 END) AS streams_search,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri = '' THEN 1 END) AS streams_undeveloped_playlist,
    COUNT (CASE WHEN stream_source = 'others_playlist' AND source_uri != '' THEN 1 END) AS streams_playlist,
    CASE WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -13 day)) and timestamp(date_add(date("2017-12-14"), interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(date("2017-12-14"), interval -6 day)) and timestamp(date("2017-12-14")) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
        WHERE _partitiontime between timestamp(date_add(date("2017-12-14"), interval - 13 day)) and timestamp(date("2017-12-14"))
              and user_country_code != 'US'
    GROUP BY user_country_code, user_country_name, region_dma_code, dma_name, isrc, week
    )
)AS t
LEFT JOIN
(SELECT isrc, canopus_id, formatted_title, resource_rollup_id
 FROM
  (SELECT isrc, canopus_id, formatted_title, resource_rollup_id,
        row_number() over (partition by isrc, canopus_id, resource_rollup_id) as rn_title
   FROM `umg-tools.metadata.canopus_resource`)
 WHERE rn_title = 1
 GROUP BY isrc, canopus_id, formatted_title, resource_rollup_id
 )  AS c
  ON c.isrc = t.isrc
LEFT JOIN
  (SELECT canopus_id, default_name
  FROM `umg-tools.metadata.canopus_name`
  GROUP BY  canopus_id, default_name) AS cn
  ON cn.canopus_id = c.canopus_id
)
WHERE track_artist is not null AND track_title is not null