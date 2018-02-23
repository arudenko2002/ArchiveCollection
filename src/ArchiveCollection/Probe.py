import sst
from sst import SstQueryDataOperator
import datetime
from datetime import datetime,timedelta, date

class Proba:

    def daterange(self,start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def runProba(self):
        pastdate="2017-10-20"
        nowdate=str(datetime.now())[:10]
        aaa=datetime.strptime(pastdate,"%Y-%m-%d")
        bbb=datetime.strptime(nowdate,"%Y-%m-%d")
        ccc=pastdate.replace("-","")
        for it in range(0,366):
            #print it
            print '{:03d}'.format(it)

        print aaa
        print bbb
        print ccc

        for single_date in self.daterange(aaa, bbb):
            print str(single_date)[:10]


        for it in range((bbb-aaa).days):
            single_date = aaa+timedelta(it)
            print str(single_date)[:10]

        sql2="SELECT " \
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
             "inner join `umg-dev.swift_trends.isrc_first_stream_date` fsd on s.isrc = fsd.isrc " \
             "where date_diff(s.stream_date, fsd.first_stream_date, DAY) <= 365 " \
             "group by " \
             "isrc, " \
             "first_stream_date, " \
             "stream_date, " \
             "day_since_first_stream, " \
             "user_country_code, " \
             "user_country_name, " \
             "stream_source"
        print sql2

        hook = sst.BigQueryHook(bigquery_conn_id="bigquery_default")

        #hook.create_table_with_partition(
        #                            "swift-trends-alerts",
        #                            "testtable")

if __name__ == '__main__':
    d = str(datetime.now()+timedelta(days=-1))[:10]
    print d
    executionDateToday=str(datetime.now())
    print "executionDateTimeNow="+executionDateToday
    actualDate = (str(datetime.now()+timedelta(hours=-2))[:13]+":00:00")
    print ("actualDateTime="+actualDate)
    partition = actualDate[2:4]+actualDate[11:13]+actualDate[4:10]+" 00:00:00"
    print partition
    print "a="+actualDate[2:4]
    #proba = Proba()
    #proba.runProba()
    #op = SstQueryDataOperator(
    #    sql="select * from `umg-dev.swift_alerts.from_mongodb_users` LIMIT 100",
    #    #bigquey_conn_id="bigquery_default",
    #    task_id="query_run"
    #)

    pass