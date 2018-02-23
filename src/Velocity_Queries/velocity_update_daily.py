from __future__ import print_function
import airflow
import logging
import sys
import pytz
import velocity_operators

from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators import VelocityStreamByDateOperator
from airflow.operators import VelocityStreamByDatePartitionListOperator
from airflow.operators import WaitQueryOperator
#from airflow.operators import WaitGCSOperator


default_args = {
    'owner': 'alexey.rudenko2002@umusic.com',
    'depends_on_past': False,
    'schedule_interval': None,
    'email': ['alexey.rudenko2002@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60)
    #,'priority_weight': 10
}

sql1 = """
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
    @datePartition as report_date,
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
    CASE WHEN _partitiontime between timestamp(date_add(@datePartition, interval -13 day)) and timestamp(date_add(@datePartition, interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(@datePartition, interval -6 day)) and timestamp(@datePartition) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
    WHERE _partitiontime between timestamp(date_add(@datePartition, interval - 13 day)) and timestamp(@datePartition)
    GROUP BY user_country_code, user_country_name, region_dma_code, dma_name, isrc, week
    )
    UNION ALL
    (SELECT
    @datePartition as report_date,
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
    CASE WHEN _partitiontime between timestamp(date_add(@datePartition, interval -13 day)) and timestamp(date_add(@datePartition, interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(@datePartition, interval -6 day)) and timestamp(@datePartition) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
    WHERE _partitiontime between timestamp(date_add(@datePartition, interval - 13 day)) and timestamp(@datePartition)
    GROUP BY user_country_code, user_country_name, region_dma_code, dma_name, isrc, week
    )
    UNION ALL
    (SELECT
    @datePartition as report_date,
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
    CASE WHEN _partitiontime between timestamp(date_add(@datePartition, interval -13 day)) and timestamp(date_add(@datePartition, interval - 7 day)) THEN 'LW'
         WHEN _partitiontime between timestamp(date_add(@datePartition, interval -6 day)) and timestamp(@datePartition) THEN 'TW'
         END AS week
    FROM `umg-partner.spotify.streams`
        WHERE _partitiontime between timestamp(date_add(@datePartition, interval - 13 day)) and timestamp(@datePartition)
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
"""

schema_out1 = [
    {'name':'report_date','type': 'DATE'},
    {'name':'canopus_id','type': 'INTEGER'},
    {'name':'resource_rollup_id','type': 'INTEGER'},
    {'name':'track_artist','type': 'STRING'},
    {'name':'track_title','type': 'STRING'},
    {'name':'country_code','type': 'STRING'},
    {'name':'country_name','type': 'STRING'},
    {'name':'region_dma_code','type': 'STRING'},
    {'name':'dma_name','type': 'STRING'},
    {'name':'isrc','type': 'STRING'},
    {'name':'week','type': 'STRING'},
    {'name':'streams','type': 'INTEGER'},
    {'name':'streams_collection','type': 'INTEGER'},
    {'name':'streams_playlist','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist','type': 'INTEGER'},
    {'name':'streams_other','type': 'INTEGER'},
    {'name':'streams_artist','type': 'INTEGER'},
    {'name':'streams_album','type': 'INTEGER'},
    {'name':'streams_search','type': 'INTEGER'}
]

sql2="""
#Step 2: trending tracks, trending countries for a track
#to build this table, use the one created after step 1

#This table will contain: top 2,000 tracks by internal UMG rank and their velocity, streams, and streams change
#for each market including Global and Global ex US
SELECT report_date, isrc,
       canopus_id, resource_rollup_id,
       track_artist, track_title, country_code, country_name,
       streams_tw, streams_lw, streams_change,
       streams_collection_tw, streams_collection_lw, streams_collection_change, 
       CASE WHEN streams_change != 0 THEN (streams_collection_change / streams_change) END AS streams_collection_change_perc,
       streams_other_tw, streams_other_lw, streams_other_change, 
       CASE WHEN streams_change != 0 THEN (streams_other_change / streams_change) END AS streams_other_change_perc,
       streams_artist_tw, streams_artist_lw, streams_artist_change, 
       CASE WHEN streams_change != 0 THEN (streams_artist_change / streams_change) END AS streams_artist_change_perc,
       streams_album_tw, streams_album_lw, streams_album_change, 
       CASE WHEN streams_change != 0 THEN (streams_album_change / streams_change) END AS streams_album_change_perc,
       streams_search_tw, streams_search_lw, streams_search_change, 
       CASE WHEN streams_change != 0 THEN (streams_search_change / streams_change) END AS streams_search_change_perc,
       streams_playlist_tw, streams_playlist_lw, streams_playlist_change, 
       CASE WHEN streams_change != 0 THEN (streams_playlist_change / streams_change) END AS streams_playlist_change_perc,
       streams_undeveloped_playlist_tw, streams_undeveloped_playlist_lw, streams_undeveloped_playlist_change, 
       CASE WHEN streams_change != 0 THEN (streams_undeveloped_playlist_change / streams_change) END AS streams_undeveloped_playlist_change_perc,
       collection_perc, playlist_perc,
       rank_tw, CASE WHEN streams_lw is null THEN null ELSE rank_lw END AS rank_lw, CASE WHEN streams_lw is null then null ELSE (rank_lw - rank_tw) END AS rank_change, 
       CASE WHEN rank_adj_score > 0 AND (streams_tw < streams_lw OR rank_tw > rank_lw)
       THEN 0 ELSE rank_adj_score END AS rank_adj_score
FROM
(
SELECT *,
CASE WHEN std_transformed_diff != 0 AND z_score_transformed_diff_divider != 0
     THEN ((transformed_diff - mean_transformed_diff)/std_transformed_diff)/z_score_transformed_diff_divider END AS rank_adj_score
FROM
(
SELECT *,
AVG(transformed_diff) OVER (partition by country_name) AS mean_transformed_diff,
STDDEV(transformed_diff) OVER (partition by country_name) AS std_transformed_diff,
ASINH(LEAST(rank_tw, rank_lw)) AS z_score_transformed_diff_divider
FROM
(
SELECT *,
ASINH(rank_lw_constrained - rank_tw_constrained) AS transformed_diff
FROM
(
SELECT *,
CASE WHEN rank_lw <= 5000 THEN rank_lw ELSE 5000 END AS rank_lw_constrained,
CASE WHEN rank_tw <= 5000 THEN rank_tw ELSE 5000 END AS rank_tw_constrained
FROM
(
SELECT *,
rank() over (partition by country_code order by streams_lw  desc) AS rank_lw,
rank() over (partition by country_code order by streams_tw desc) AS rank_tw,
(streams_tw - streams_lw) AS streams_change,
(streams_collection_tw - streams_collection_lw) AS streams_collection_change,
(streams_other_tw - streams_other_lw) AS streams_other_change,
(streams_artist_tw - streams_artist_lw) AS streams_artist_change,
(streams_album_tw - streams_album_lw) AS streams_album_change,
(streams_search_tw - streams_search_lw) AS streams_search_change,
(streams_playlist_tw - streams_playlist_lw) AS streams_playlist_change,
(streams_undeveloped_playlist_tw - streams_undeveloped_playlist_lw) AS streams_undeveloped_playlist_change,
CASE WHEN streams_tw != 0 THEN streams_collection_tw/streams_tw ELSE null END AS collection_perc,
CASE WHEN streams_tw != 0 THEN streams_playlist_tw/streams_tw ELSE null END AS playlist_perc
FROM
(
SELECT report_date, isrc, canopus_id, resource_rollup_id, track_artist, track_title, country_code, country_name,
SUM(CASE WHEN week = 'LW' THEN streams END) AS streams_lw,
SUM(CASE WHEN week = 'TW' THEN streams END) AS streams_tw,
SUM(CASE WHEN week = 'TW' THEN streams_collection END) AS streams_collection_tw,
SUM(CASE WHEN week = 'LW' THEN streams_collection END) AS streams_collection_lw,
SUM(CASE WHEN week = 'TW' THEN streams_other END) AS streams_other_tw,
SUM(CASE WHEN week = 'LW' THEN streams_other END) AS streams_other_lw,
SUM(CASE WHEN week = 'TW' THEN streams_artist END) AS streams_artist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_artist END) AS streams_artist_lw,
SUM(CASE WHEN week = 'TW' THEN streams_album END) AS streams_album_tw,
SUM(CASE WHEN week = 'LW' THEN streams_album END) AS streams_album_lw,
SUM(CASE WHEN week = 'TW' THEN streams_search END) AS streams_search_tw,
SUM(CASE WHEN week = 'LW' THEN streams_search END) AS streams_search_lw,
SUM(CASE WHEN week = 'TW' THEN streams_undeveloped_playlist END) AS streams_undeveloped_playlist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_undeveloped_playlist END) AS streams_undeveloped_playlist_lw,
SUM(CASE WHEN week = 'TW' THEN streams_playlist END) AS streams_playlist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_playlist END) AS streams_playlist_lw
FROM  `@1_velocity_base_table` --the table from the step 1_velocity_base_table
WHERE _partitiontime = timestamp(@datePartition)
GROUP BY report_date, isrc, canopus_id, resource_rollup_id, track_artist, track_title, country_code, country_name
)
))))
WHERE rank_tw <= 2000)
"""

schema_out2 = [
    {'name':'report_date','type': 'DATE'},
    {'name':'isrc','type': 'STRING'},
    {'name':'canopus_id','type': 'INTEGER'},
    {'name':'resource_rollup_id','type': 'INTEGER'},
    {'name':'track_artist','type': 'STRING'},
    {'name':'track_title','type': 'STRING'},
    {'name':'country_code','type': 'STRING'},
    {'name':'country_name','type': 'STRING'},
    #{'name':'region_dma_code','type': 'STRING'},
    #{'name':'dma_name','type': 'STRING'},
    {'name':'streams_tw','type': 'INTEGER'},
    {'name':'streams_lw','type': 'INTEGER'},
    {'name':'streams_change','type': 'INTEGER'},
    {'name':'streams_collection_tw','type': 'INTEGER'},
    {'name':'streams_collection_lw','type': 'INTEGER'},
    {'name':'streams_collection_change','type': 'INTEGER'},
    {'name':'streams_collection_change_perc','type': 'FLOAT'},
    {'name':'streams_other_tw','type': 'INTEGER'},
    {'name':'streams_other_lw','type': 'INTEGER'},
    {'name':'streams_other_change','type': 'INTEGER'},
    {'name':'streams_other_change_perc','type': 'FLOAT'},
    {'name':'streams_artist_tw','type': 'INTEGER'},
    {'name':'streams_artist_lw','type': 'INTEGER'},
    {'name':'streams_artist_change','type': 'INTEGER'},
    {'name':'streams_artist_change_perc','type': 'FLOAT'},
    {'name':'streams_album_tw','type': 'INTEGER'},
    {'name':'streams_album_lw','type': 'INTEGER'},
    {'name':'streams_album_change','type': 'INTEGER'},
    {'name':'streams_album_change_perc','type': 'FLOAT'},
    {'name':'streams_search_tw','type': 'INTEGER'},
    {'name':'streams_search_lw','type': 'INTEGER'},
    {'name':'streams_search_change','type': 'INTEGER'},
    {'name':'streams_search_change_perc','type': 'FLOAT'},
    {'name':'streams_playlist_tw','type': 'INTEGER'},
    {'name':'streams_playlist_lw','type': 'INTEGER'},
    {'name':'streams_playlist_change','type': 'INTEGER'},
    {'name':'streams_playlist_change_perc','type': 'FLOAT'},
    {'name':'streams_undeveloped_playlist_tw','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_lw','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_change','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_change_perc','type': 'FLOAT'},
    {'name':'collection_perc','type': 'FLOAT'},
    {'name':'playlist_perc','type': 'FLOAT'},
    {'name':'rank_tw','type': 'INTEGER'},
    {'name':'rank_lw','type': 'INTEGER'},
    {'name':'rank_change','type': 'INTEGER'},
    {'name':'rank_adj_score','type': 'FLOAT'}
]

sql3="""
    # Trending regions/DMAs for a track, trending tracks for each region/DMA
SELECT report_date, isrc, 
       canopus_id, resource_rollup_id,
       track_artist, track_title,
       country_code, country_name, region_dma_code, dma_name,
       streams_tw, streams_lw, streams_change,
       streams_collection_tw, streams_collection_lw, streams_collection_change, 
       CASE WHEN streams_change != 0 THEN (streams_collection_change / streams_change) END AS streams_collection_change_perc,
       streams_other_tw, streams_other_lw, streams_other_change, 
       CASE WHEN streams_change != 0 THEN (streams_other_change / streams_change) END AS streams_other_change_perc,
       streams_artist_tw, streams_artist_lw, streams_artist_change, 
       CASE WHEN streams_change != 0 THEN (streams_artist_change / streams_change) END AS streams_artist_change_perc,
       streams_album_tw, streams_album_lw, streams_album_change, 
       CASE WHEN streams_change != 0 THEN (streams_album_change / streams_change) END AS streams_album_change_perc,
       streams_search_tw, streams_search_lw, streams_search_change, 
       CASE WHEN streams_change != 0 THEN (streams_search_change / streams_change) END AS streams_search_change_perc,
       streams_playlist_tw, streams_playlist_lw, streams_playlist_change, 
       CASE WHEN streams_change != 0 THEN (streams_playlist_change / streams_change) END AS streams_playlist_change_perc,
       streams_undeveloped_playlist_tw, streams_undeveloped_playlist_lw, streams_undeveloped_playlist_change, 
       CASE WHEN streams_change != 0 THEN (streams_undeveloped_playlist_change / streams_change) END AS streams_undeveloped_playlist_change_perc,
       collection_perc, playlist_perc,
       rank_tw, CASE WHEN streams_lw is null THEN null ELSE rank_lw END AS rank_lw, CASE WHEN streams_lw is null then null ELSE (rank_lw - rank_tw) END AS rank_change, 
       CASE WHEN rank_adj_score > 0 AND (streams_tw < streams_lw OR rank_tw > rank_lw)
            THEN 0 ELSE rank_adj_score END AS rank_adj_score
FROM
(
SELECT *,
CASE WHEN std_transformed_diff != 0 AND z_score_transformed_diff_divider != 0
     THEN((transformed_diff - mean_transformed_diff)/std_transformed_diff)/z_score_transformed_diff_divider END AS rank_adj_score
FROM
(
SELECT *,
AVG(transformed_diff) OVER (partition by country_code, region_dma_code) AS mean_transformed_diff,
STDDEV(transformed_diff) OVER (partition by country_code, region_dma_code) AS std_transformed_diff,
ASINH(LEAST(rank_tw, rank_lw)) AS z_score_transformed_diff_divider
FROM
(
SELECT *,
ASINH(rank_lw_constrained - rank_tw_constrained) AS transformed_diff
FROM
(
SELECT *,
CASE WHEN rank_lw <= 5000 THEN rank_lw ELSE 5000 END AS rank_lw_constrained,
CASE WHEN rank_tw <= 5000 THEN rank_tw ELSE 5000 END AS rank_tw_constrained
FROM
(SELECT *,
rank() over (partition by country_code, region_dma_code order by streams_lw  desc) AS rank_lw,
rank() over (partition by country_code, region_dma_code order by streams_tw desc) AS rank_tw,
(streams_tw - streams_lw) AS streams_change,
(streams_collection_tw - streams_collection_lw) AS streams_collection_change,
(streams_other_tw - streams_other_lw) AS streams_other_change,
(streams_artist_tw - streams_artist_lw) AS streams_artist_change,
(streams_album_tw - streams_album_lw) AS streams_album_change,
(streams_search_tw - streams_search_lw) AS streams_search_change,
(streams_playlist_tw - streams_playlist_lw) AS streams_playlist_change,
(streams_undeveloped_playlist_tw - streams_undeveloped_playlist_lw) AS streams_undeveloped_playlist_change,
CASE WHEN streams_tw != 0 THEN streams_collection_tw/streams_tw ELSE null END AS collection_perc,
CASE WHEN streams_tw != 0 THEN streams_playlist_tw/streams_tw ELSE null END AS playlist_perc

FROM
(SELECT report_date, isrc, canopus_id, resource_rollup_id, track_artist, track_title, country_code, country_name,
        region_dma_code, dma_name,
SUM(CASE WHEN week = 'LW' THEN streams END) AS streams_lw,
SUM(CASE WHEN week = 'TW' THEN streams END) AS streams_tw,
SUM(CASE WHEN week = 'TW' THEN streams_collection END) AS streams_collection_tw,
SUM(CASE WHEN week = 'LW' THEN streams_collection END) AS streams_collection_lw,
SUM(CASE WHEN week = 'TW' THEN streams_other END) AS streams_other_tw,
SUM(CASE WHEN week = 'LW' THEN streams_other END) AS streams_other_lw,
SUM(CASE WHEN week = 'TW' THEN streams_artist END) AS streams_artist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_artist END) AS streams_artist_lw,
SUM(CASE WHEN week = 'TW' THEN streams_album END) AS streams_album_tw,
SUM(CASE WHEN week = 'LW' THEN streams_album END) AS streams_album_lw,
SUM(CASE WHEN week = 'TW' THEN streams_search END) AS streams_search_tw,
SUM(CASE WHEN week = 'LW' THEN streams_search END) AS streams_search_lw,
SUM(CASE WHEN week = 'TW' THEN streams_undeveloped_playlist END) AS streams_undeveloped_playlist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_undeveloped_playlist END) AS streams_undeveloped_playlist_lw,
SUM(CASE WHEN week = 'TW' THEN streams_playlist END) AS streams_playlist_tw,
SUM(CASE WHEN week = 'LW' THEN streams_playlist END) AS streams_playlist_lw
FROM `@1_velocity_base_table` --the table from the step 1_velocity_base_table
WHERE region_dma_code != '' and _partitiontime = timestamp(@datePartition)
        and ((country_code = 'SE' and SUBSTR(region_dma_code , 1, 2)='SE' )
        or (country_code = 'AR' and SUBSTR(region_dma_code , 1, 2)='AR' )
        or (country_code = 'AU' and SUBSTR(region_dma_code , 1, 2)='AU' )
        or (country_code = 'BE' and SUBSTR(region_dma_code , 1, 2)='BE' )
        or (country_code = 'BR' and SUBSTR(region_dma_code , 1, 2)='BR' )
        or (country_code = 'US' and REGEXP_CONTAINS(region_dma_code, r'^[0-9]')=true)
        or (country_code = 'DE' and SUBSTR(region_dma_code , 1, 2)='DE' )
        or (country_code = 'DK' and SUBSTR(region_dma_code , 1, 2)='DK' )
        or (country_code = 'NO' and SUBSTR(region_dma_code , 1, 2)='NO' )
        or (country_code = 'ES' and SUBSTR(region_dma_code , 1, 2)='ES' )
        or (country_code = 'FI' and SUBSTR(region_dma_code , 1, 2)='FI' )
        or (country_code = 'FR' and SUBSTR(region_dma_code , 1, 2)='FR' )
        or (country_code = 'GB' and SUBSTR(region_dma_code , 1, 2)='GB' )
        or (country_code = 'IT' and SUBSTR(region_dma_code , 1, 2)='IT' )
        or (country_code = 'MX' and SUBSTR(region_dma_code , 1, 2)='MX' )
        or (country_code = 'NL' and SUBSTR(region_dma_code , 1, 2)='NL' ))

GROUP BY report_date, isrc, canopus_id, resource_rollup_id, track_artist, track_title,
         country_code, country_name, region_dma_code, dma_name)
         ))))
WHERE rank_tw <= 2000)
"""

schema_out3 = [
    {'name':'report_date','type': 'DATE'},
    {'name':'isrc','type': 'STRING'},
    {'name':'canopus_id','type': 'INTEGER'},
    {'name':'resource_rollup_id','type': 'INTEGER'},
    {'name':'track_artist','type': 'STRING'},
    {'name':'track_title','type': 'STRING'},
    {'name':'country_code','type': 'STRING'},
    {'name':'country_name','type': 'STRING'},
    {'name':'region_dma_code','type': 'STRING'},
    {'name':'dma_name','type': 'STRING'},
    {'name':'streams_tw','type': 'INTEGER'},
    {'name':'streams_lw','type': 'INTEGER'},
    {'name':'streams_change','type': 'INTEGER'},
    {'name':'streams_collection_tw','type': 'INTEGER'},
    {'name':'streams_collection_lw','type': 'INTEGER'},
    {'name':'streams_collection_change','type': 'INTEGER'},
    {'name':'streams_collection_change_perc','type': 'FLOAT'},
    {'name':'streams_other_tw','type': 'INTEGER'},
    {'name':'streams_other_lw','type': 'INTEGER'},
    {'name':'streams_other_change','type': 'INTEGER'},
    {'name':'streams_other_change_perc','type': 'FLOAT'},
    {'name':'streams_artist_tw','type': 'INTEGER'},
    {'name':'streams_artist_lw','type': 'INTEGER'},
    {'name':'streams_artist_change','type': 'INTEGER'},
    {'name':'streams_artist_change_perc','type': 'FLOAT'},
    {'name':'streams_album_tw','type': 'INTEGER'},
    {'name':'streams_album_lw','type': 'INTEGER'},
    {'name':'streams_album_change','type': 'INTEGER'},
    {'name':'streams_album_change_perc','type': 'FLOAT'},
    {'name':'streams_search_tw','type': 'INTEGER'},
    {'name':'streams_search_lw','type': 'INTEGER'},
    {'name':'streams_search_change','type': 'INTEGER'},
    {'name':'streams_search_change_perc','type': 'FLOAT'},
    {'name':'streams_playlist_tw','type': 'INTEGER'},
    {'name':'streams_playlist_lw','type': 'INTEGER'},
    {'name':'streams_playlist_change','type': 'INTEGER'},
    {'name':'streams_playlist_change_perc','type': 'FLOAT'},
    {'name':'streams_undeveloped_playlist_tw','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_lw','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_change','type': 'INTEGER'},
    {'name':'streams_undeveloped_playlist_change_perc','type': 'FLOAT'},
    {'name':'collection_perc','type': 'FLOAT'},
    {'name':'playlist_perc','type': 'FLOAT'},
    {'name':'rank_tw','type': 'INTEGER'},
    {'name':'rank_lw','type': 'INTEGER'},
    {'name':'rank_change','type': 'INTEGER'},
    {'name':'rank_adj_score','type': 'FLOAT'}
]



yesterday = str(datetime.now()+timedelta(days=-1))[:10]
#yesterday="2018-01-24"
today = str(datetime.now())[:10]
#today = "2018-01-25"
today_prime = str(datetime.now()+timedelta(days=-1))[:10]

aux_table = "umg-swift.swift_alerts.velocity_base_table_v2"
dest_table_countries = "umg-swift.swift_alerts.trending_tracks_countries"
dest_table_regions = "umg-swift.swift_alerts.trending_tracks_regions"

sql_wait="SELECT * FROM `"+aux_table+"` WHERE _PARTITIONTIME='"+today_prime+"' LIMIT 2"
print("wait_sql="+sql_wait)

#declare DAG
dagvelocitydaily = DAG('velocity_queries_daily'
                      ,description='Builds velocity daily by daily increments'
                      ,start_date=datetime(2017, 12, 14, 0, 0, 0)
                      ,schedule_interval = "30 15 * * *"
                       #,schedule_interval = "None"
                      ,default_args=default_args)
dagvelocitydaily.catchup=False

task_streams_daily_velocity_base = VelocityStreamByDateOperator(
    task_id="create_release_table_daily",
    #schema_out = schema_out1,
    schema_out = None,
    sql = sql1.replace("@datePartition",'date("{datePartition}")'),
    destination_table=aux_table,
    start_date=yesterday,
    end_date=today,
    dag=dagvelocitydaily
)

#task_sleep = BashOperator(
#    task_id='sleep',
#    bash_command='sleep 1200',
#    dag=dagvelocitydaily
#)

task_wait_bq = WaitQueryOperator(
    task_id="wait_for_query",
    sql = sql_wait,
    dag=dagvelocitydaily
)

task_streams_daily_countries = VelocityStreamByDateOperator(
    task_id="create_trend_daily_countries",
    sql = sql2.replace("@datePartition",'date("{datePartition}")').replace("@1_velocity_base_table",aux_table),
    destination_table=dest_table_countries,
    #schema_out = schema_out2,
    schema_out = None,
    start_date=yesterday,
    end_date=today,
    dag=dagvelocitydaily
)

task_streams_daily_regions = VelocityStreamByDateOperator(
    task_id="create_trend_daily_regions",
    sql = sql3.replace("@datePartition",'date("{datePartition}")').replace("@1_velocity_base_table",aux_table),
    destination_table=dest_table_regions,
    #schema_out = schema_out3,
    schema_out = None,
    start_date=yesterday,
    end_date=today,
    dag=dagvelocitydaily
)

task_streams_daily_velocity_base >> task_wait_bq >> task_streams_daily_countries >> task_streams_daily_regions