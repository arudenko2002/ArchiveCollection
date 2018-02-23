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
       CASE WHEN streams_change != 0 THEN (streams_undeveloped_playlist_change / streams_change) END AS streams_udeveloped_playlist_change_perc,
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
FROM  `umg-swift.swift_alerts.velocity_base_table_v2` --the table from the step 1_velocity_base_table
WHERE _partitiontime = timestamp(date("2017-12-14"))
GROUP BY report_date, isrc, canopus_id, resource_rollup_id, track_artist, track_title, country_code, country_name
)
))))
WHERE rank_tw <= 2000)