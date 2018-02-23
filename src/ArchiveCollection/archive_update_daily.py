from __future__ import print_function
import airflow
import logging
import sys
import pytz
import archive_operators

from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import CreateReleaseOperator
from airflow.operators import StreamByDateOperator
from airflow.operators import StreamByDatePartitionListOperator
from airflow.operators import ArchiveByDateOperator

default_args = {
    'owner': 'alexey.rudenko2002@umusic.com',
    'depends_on_past': False,
    'schedule_interval': None,
    'email': ['alexey.rudenko2002@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60)
}

sql1 = "SELECT isrc, MIN(stream_date) as first_stream_date from `umg-partner.spotify.daily_track_history` group by isrc"

sql22="SELECT " \
     "s.isrc as isrc, " \
     "fsd.first_stream_date as first_stream_date, " \
     "s.stream_date as stream_date, " \
     "date_diff(s.stream_date, fsd.first_stream_date, DAY) as day_since_first_stream, " \
     "s.user_country_code as user_country_code, " \
     "s.user_country_name as user_country_name, " \
     "s.stream_source as stream_source, " \
     "count(user_id) as total_stream_count, " \
     "count(case when engagement_style = 'Lean Back' then 1 end) as lean_back_stream_count, " \
     "count(case when engagement_style = 'Lean Forward' then 1 end) as lean_forward_stream_count, " \
     "current_timestamp() as load_datetime " \
     "from ( " \
     "select * FROM `umg-partner.spotify.streams` " \
     "where _PARTITIONTIME = timestamp(\"{datePartition}\")) s " \
     "inner join `{project}.swift_alerts.isrc_first_stream_date` fsd on s.isrc = fsd.isrc " \
     "where date_diff(s.stream_date, fsd.first_stream_date, DAY) <= 365 " \
     "group by " \
     "isrc, " \
     "first_stream_date, " \
     "stream_date, " \
     "day_since_first_stream, " \
     "user_country_code, " \
     "user_country_name, " \
     "stream_source"

sql2="""
-- Step 3 - calculate lean back streams as streams - lean forward streams
-- remove duplicates
select isrc,
    first_stream_date
    stream_date,
    day_since_first_stream,
    user_country_code,
    user_country_name,
    stream_source,
    stream_count,
    lean_forward_stream_count,
    (stream_count - lean_forward_stream_count) AS lean_back_stream_count,
    users,
    users_isrc_day_sos,
    users_isrc_day_lf,
    users_isrc_day,
    load_datetime
from
(
-- Step 2 - count streams, lf streams, and distinct users
SELECT
    s.isrc,
    fsd.first_stream_date as first_stream_date,
    s.stream_date,
    date_diff(s.stream_date,fsd.first_stream_date, DAY) as day_since_first_stream,
    s.user_country_code,
    s.user_country_name,
    s.stream_source,
    
    count(1) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as stream_count,
    count(case when engagement_style = 'Lean Forward' then 1 end) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as lean_forward_stream_count,
    
    
    count(distinct user_id) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as users,
    count(distinct user_id) over (partition by s.isrc, s.stream_date, s.stream_source) as users_isrc_day_sos,
    count(distinct case when engagement_style = 'Lean Forward' then user_id end) over (partition by s.isrc, s.stream_date) as users_isrc_day_lf,
    count(distinct user_id) over (partition by s.isrc, s.stream_date) as users_isrc_day,
  
    current_timestamp() as load_datetime
    from (
--     Step 1 - Split others playlist into playlist and undeveloped playlist
        select isrc, stream_date, user_id, user_country_code, user_country_name, engagement_style,
           CASE WHEN stream_source = "others_playlist" and source_uri != "" THEN 'playlist'
           WHEN stream_source = "others_playlist" and source_uri = "" THEN 'undeveloped_playlist'
           ELSE stream_source
           END as stream_source
        from `umg-partner.spotify.streams` 
        where _PARTITIONTIME = timestamp("{datePartition}")
    ) s
    inner join `umg-dev.swift_alerts.isrc_first_stream_date` fsd
    ON s.isrc=fsd.isrc
    where date_diff(s.stream_date, fsd.first_stream_date, DAY) <=365
--    group by isrc,
--    first_stream_date,
--    day_sicnce_first_stream,
--    user_country_code,
--    user_country_name,
--    stream_source
)
group by isrc, 
    first_stream_date, 
    stream_date, 
    day_since_first_stream, 
    user_country_code, 
    user_country_name, 
    stream_source, 
    --stream_count, 
    --lean_forward_stream_count, 
    --users, 
    --users_isrc_day_sos, 
    --users_isrc_day_lf, 
    --users_isrc_day, 
    --load_datetime
"""

sql2_alternative = """
-- Step 3 - calculate lean back streams as streams - lean forward streams
-- remove duplicates
select isrc,
    first_stream_date
    stream_date,
    day_since_first_stream,
    user_country_code,
    user_country_name,
    stream_source,
    stream_count,
    lean_forward_stream_count,
    (stream_count - lean_forward_stream_count) AS lean_back_stream_count,
    users,
    users_isrc_day_sos,
    users_isrc_day_lf,
    --users_isrc_day,
    load_datetime
from
(
-- Step 2 - count streams, lf streams, and distinct users
SELECT
    s.isrc,
    fsd.first_stream_date as first_stream_date,
    s.stream_date,
    date_diff(s.stream_date,fsd.first_stream_date, DAY) as day_since_first_stream,
    s.user_country_code,
    s.user_country_name,
    s.stream_source,
    
    count(1) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as stream_count,
    count(case when engagement_style = 'Lean Forward' then 1 end) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as lean_forward_stream_count,
    
    
    count(distinct user_id) over (partition by s.isrc, s.stream_date, s.user_country_code, s.stream_source) as users,
    count(distinct user_id) over (partition by s.isrc, s.stream_date, s.stream_source) as users_isrc_day_sos,
    count(distinct case when engagement_style = 'Lean Forward' then user_id end) over (partition by s.isrc, s.stream_date) as users_isrc_day_lf,
    --count(distinct user_id) over (partition by s.isrc, s.stream_date) as users_isrc_day,
  
    current_timestamp() as load_datetime
    from (
--     Step 1 - Split others playlist into playlist and undeveloped playlist
        select isrc, stream_date, user_id, user_country_code, user_country_name, engagement_style,
           CASE WHEN stream_source = "others_playlist" and source_uri != "" THEN 'playlist'
           WHEN stream_source = "others_playlist" and source_uri = "" THEN 'undeveloped_playlist'
           ELSE stream_source
           END as stream_source
        from `umg-partner.spotify.streams` 
        where _PARTITIONTIME = timestamp("{datePartition}")
    ) s
    inner join `umg-dev.swift_alerts.isrc_first_stream_date` fsd
    ON s.isrc=fsd.isrc
    where date_diff(s.stream_date, fsd.first_stream_date, DAY) <=365
--    group by isrc,
--    first_stream_date,
--    day_sicnce_first_stream,
--    user_country_code,
--    user_country_name,
--    stream_source
)
group by isrc, 
    first_stream_date, 
    stream_date, 
    day_since_first_stream, 
    user_country_code, 
    user_country_name, 
    stream_source, 
    --stream_count, 
    --lean_forward_stream_count, 
    --users, 
    --users_isrc_day_sos, 
    --users_isrc_day_lf, 
    ----users_isrc_day, 
    --load_datetime
"""

sql2list="SELECT * FROM " \
    "( " \
        "SELECT DATE(partner_report_datetime) as partner_load_date, max(load_datetime) as created_date, count(*) as record_count " \
        "FROM `umg-partner.spotify.streams` " \
        "GROUP BY 1 " \
    ") a " \
    "LEFT JOIN " \
    "( " \
        "SELECT stream_date as our_load_date, max(load_datetime) as our_created_date,count(*) as our_record_count " \
        "FROM `{project}.swift_alerts.track_archive_by_stream_date` " \
        "GROUP BY 1 " \
    ") b " \
    "ON a.partner_load_date=b.our_load_date " \
    "WHERE (b.our_created_date is NULL OR a.created_date>b.our_created_date) " \
    "AND partner_load_date>='2013-09-01' " \
    "ORDER BY partner_load_date DESC " \
    "LIMIT 1000"

sql3="""
    select *
    from `{project}.swift_alerts.track_archive_by_stream_date`
    where day_since_first_stream = cast("{daySinceFirstStream}" as int64)
"""

schema_out= [
    {'name':'isrc','type': 'STRING'},
    {'name': 'first_stream_date','type': 'DATE'},
    {'name': 'stream_date','type': 'DATE'},
    {'name': 'day_since_first_stream','type': 'INTEGER'},
    {'name': 'user_country_code','type': 'STRING'},
    {'name': 'user_country_name','type': 'STRING'},
    {'name': 'stream_source','type': 'STRING'},
    {'name': 'stream_count','type': 'INTEGER'},
    {'name': 'lean_forward_stream_count','type': 'INTEGER'},
    {'name': 'lean_back_stream_count','type': 'INTEGER'},
    {'name': 'users','type': 'INTEGER'},
    {'name': 'users_isrc_day_sos','type': 'INTEGER'},
    {'name': 'users_isrc_day_lf','type': 'INTEGER'},
    {'name': 'users_isrc_day','type': 'INTEGER'},
    {'name': 'load_datetime','type': 'TIMESTAMP'}
]

#declare DAG
dagarchivedaily = DAG('archive_first_year_daily'
              ,description='Builds archive by daily increments'
              ,start_date=datetime(2017, 12, 20, 0, 0, 0)
              #,start_date=datetime.now()+timedelta(hours=-2)
              ,schedule_interval = "0 16 * * *"
              ,default_args=default_args)
dagarchivedaily.catchup=False

task_release_daily = CreateReleaseOperator(
   task_id="create_release_table_daily",
   sql = sql1,
   destination_table="umg-dev.swift_alerts.isrc_first_stream_date",
   dag=dagarchivedaily
)

# task_streams_daily = StreamByDateOperator(
#     task_id="create_stream_by_date_daily",
#     sql = sql2,
#     destination_table="umg-dev.swift_alerts.track_archive_by_stream_date",
#     #start_date="2017-10-26",
#     start_date=str(datetime.now(pytz.utc)+timedelta(days=-1))[:10],   # for daily processing - run yesterday only
#     dag=dagarchivedaily
# )

task_streams_daily_partition_list = StreamByDatePartitionListOperator(
    task_id="create_stream_by_date_daily",
    sql = sql2,
    sql_alternative = sql2_alternative,
    partition_list_sql = sql2list,
    destination_table="umg-dev.swift_alerts.track_archive_by_stream_date",
    #schema_out = schema_out,
    schema_out = None,
    dag=dagarchivedaily
)

task_archive_daily = ArchiveByDateOperator(
    task_id="create_archive_by_date_daily",
    sql = sql3,
    destination_table="umg-dev.swift_alerts.track_archive",
    dag=dagarchivedaily
)

task_release_daily >> task_streams_daily_partition_list >> task_archive_daily