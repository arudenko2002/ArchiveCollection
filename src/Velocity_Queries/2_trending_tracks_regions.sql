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
       CASE WHEN streams_change != 0 THEN (streams_undeveloped_playlist_change / streams_change) END AS streams_udeveloped_playlist_change_perc,
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
FROM `umg-swift.swift_alerts.velocity_base_table_v2` --the table from the step 1_velocity_base_table
WHERE region_dma_code != '' and _partitiontime = timestamp(date("2017-12-14"))
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