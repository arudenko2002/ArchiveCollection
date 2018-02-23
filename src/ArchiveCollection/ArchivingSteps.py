import sst
from sst import BigQueryHook

class ArchivingSteps:
    def createReleaseTable(self):
        print "Create release table"
        print "Create temporary table"
        print "Create output table"

    def getQuery(self):
        sql="WITH  getToday AS (" \
    "\n--SELECT CURRENT_DATE()\n" \
        "SELECT DATE(\"{ExecutionDate}\") as today" \
    ")," \
"\n-- writes umg-dev.swift_trends.isrc_first_stream_date \n" \
    "get_release_dates AS( " \
        "SELECT * FROM isrc, MIN(stream_date) as first_stream_date " \
            "from `umg-partner.spotify.daily_track_history` " \
            "group by isrc " \
        ")," \
"\n-- writes to umg-dev.swift_trends.track_archive_by_stream_date\$date \n" \
    "set_daily_history AS ( " \
        "SELECT " \
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
        "FROM (" \
            "SELECT * FROM `umg-partner.spotify.streams` " \
                "WHERE _PARTITIONTIME = TIMESTAMP(\"{DATE}\") " \
            ") s " \
            "INNER JOIN ( " \
                "SELECT * FROM isrc, MIN(stream_date) as first_stream_date \n" \
                "from `umg-partner.spotify.daily_track_history` " \
                "group by isrc " \
            ") fsd ON s.isrc = fsd.isrc " \
            "WHERE date_diff(s.stream_date, fsd.first_stream_date, DAY) <= 365 " \
            "GROUP BY " \
                "isrc, " \
                "first_stream_date, " \
                "stream_date, " \
                "day_since_first_stream, " \
                "user_country_code, " \
                "user_country_name, " \
                "stream_source " \
            ")," \
"\n-- writes to umg-dev.swift_trends.track_archive\$1xxx-01-01 \n" \
    "get_daily_history AS ( " \
        "SELECT * " \
        "FROM `umg-dev.swift_trends.track_archive_by_stream_date` " \
        "WHERE day_since_first_stream = CAST({\"DaySinceFirstStream\"} AS int64)) \n"
        return sql


if __name__ == '__main__':
    print "Start archiving"
    p=ArchivingSteps()
    p.createReleaseTable()
    sql=p.getQuery()
    print sql
    hook=BigQueryHook()
    destination_table="umg-dev.swift_trends_alerts.isrc_first_stream_date"
    dst_table_array=destination_table.split(".")
    dst_table = dst_table_array[len(dst_table_array)-1]
    dst_dataset = dst_table_array[len(dst_table_array)-2]
    query = sql+"  SELECT * FROM get_release_dates"
    query ="SELECT isrc, MIN(stream_date) as first_stream_date from `umg-partner.spotify.daily_track_history` group by isrc"
    #hook.client(project_id="umg-dev",json_key_file="/Users/rudenka/Downloads/UMGDevCreds.json")
    hook.write_to_table(query,dst_dataset, dst_table,use_legacy_sql=False, write_disposition='WRITE_TRUNCATE')
    pass