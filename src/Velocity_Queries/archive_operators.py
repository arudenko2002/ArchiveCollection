import logging
import time
from datetime import datetime,timedelta
import uuid
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators import BaseOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from bigquery import get_client
from bigquery.errors import BigQueryTimeoutException,JobInsertException


#silence some annoying warnings
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

#set logging level for the plugin
logging.getLogger(__name__).setLevel(logging.INFO)

class BigQueryHookAR(GoogleCloudBaseHook, DbApiHook):

    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.
    """
    conn_name_attr = 'bigquery_conn_id'
    project_id='umg-dev'
    json_key_file='/Users/rudenka/Downloads/UMGDevCreds.json'

    def __init__(self,
                 bigquery_conn_id='bigquery_default'):
        super(BigQueryHookAR, self).__init__(
            conn_id=bigquery_conn_id)


    def client(self):

        """
        Returns a BigQuery PEP 249 connection object.
        """
        project = self._get_field('project')
        json_key_file = self._get_field('key_path')
        self.project_id=project

        logging.info('project: %s', project)
        logging.info('json_key_file: %s', json_key_file)
        return get_client(project_id=project,
                          json_key_file=json_key_file,
                          readonly=False)

    def execute_query(self,
                      sql,
                      use_legacy_sql=False):


        job_id, _results=self.client().query(query=sql,
                                             use_legacy_sql=use_legacy_sql, allow_large_results=True)

        return job_id


    def fetch(self, job_id):
        complete = False
        sec = 0
        while not complete:
            complete, row_count = self.client().check_job(job_id)
            time.sleep(1)
            sec += 1

        results = self.client().get_query_rows(job_id)

        if complete:
            logging.info("Query completed in {} sec".format(sec))
        else:
            logging.info("Query failed")

        logging.info('results: %s', results)

        return results

    def fetchone(self, job_id):

        return self.fetch(job_id)[0]

    def create_table_with_partition(self,
                                    dataset,
                                    table,
                                    schema_out):
        if(not self.client().check_table(dataset,table)):
            logging.info("Creating "+dataset+"."+table)
            self.client().create_table(dataset,table,schema=schema_out,expiration_time=None,time_partitioning=True)
        else:
            logging.info("The table "+dataset+"."+table+" already exists.")


    def write_to_table(self,
                       sql,
                       destination_dataset,
                       destination_table,
                       use_legacy_sql = False,
                       write_disposition='WRITE_TRUNCATE'
                       ):

        job = self.client().write_to_table(query=sql,
                                           dataset=destination_dataset,
                                           table=destination_table,
                                           use_legacy_sql=use_legacy_sql,
                                           write_disposition=write_disposition,
                                           maximum_billing_tier=5, allow_large_results=True
                                           )
        return job

    def write_to_table_wait(self,
                           sql,
                           destination_dataset,
                           destination_table,
                           use_legacy_sql = False,
                           write_disposition='WRITE_TRUNCATE'
                           ):

        job = self.client().write_to_table(query=sql,
                                           dataset=destination_dataset,
                                           table=destination_table,
                                           use_legacy_sql=use_legacy_sql,
                                           write_disposition=write_disposition,
                                           maximum_billing_tier=5,  allow_large_results=True
                                           )
        try:
            job_resource = self.client().wait_for_job(job, timeout=3600)
            logging.info("Job completed: {}".format(job_resource))

        except BigQueryTimeoutException:
            logging.info("Query Timeout")
        return

    def get_status(self,job_record):
        complete = self.client().check_job(job_record["id"].split(":")[1])
        return complete

    def export_to_gcs(self,
                      dataset,
                      table,
                      gcs_uri):



        job = self.client().export_data_to_uris( [gcs_uri],
                                                 dataset,
                                                 table,
                                                 destination_format='NEWLINE_DELIMITED_JSON')
        try:
            job_resource = self.client().wait_for_job(job, timeout=600)
            logging.info('Export job: %s', job_resource)
        except BigQueryTimeoutException:
            logging.info('Timeout occured while exporting table %s.%s to %s',
                         dataset,
                         table,
                         gcs_uri)

class CreateReleaseOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary
    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'destination_table',
                       )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_table,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 *args,
                 **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        super(CreateReleaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]
        hook.write_to_table_wait(self.sql,dst_dataset, dst_table,use_legacy_sql=False, write_disposition='WRITE_TRUNCATE')

class StreamByDateOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary
    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'destination_table',
                       )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_table,
                 start_date,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 schema_out=[],
                 *args,
                 **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        self.start_date222=start_date
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.schema_out = schema_out
        super(StreamByDateOperator, self).__init__(*args, **kwargs)

    def clean_jobs(self,jobs,hook):
        #hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        output = []
        for job in jobs:
            status=hook.get_status(job)
            print "STATUS="+str(status)+"  "+str(job)+"  "+str(len(jobs))
            if not status:
                output.append(job)
        print("Running jobs="+str(len(output)))
        return output


    def execute(self, context):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        jobs=[]
        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]
        print "::::::::::::::::::::::::::::::::::Schema_out::::::::::::::::::::::::::::::::::::::::::::::::::::::::::"
        print self.schema_out
        hook.create_table_with_partition(
            dst_dataset,
            dst_table,self.schema_out)
        #self.start_date="2013-09-01"
        end_date=str(datetime.now())[:10]
        start = datetime.strptime(self.start_date222,"%Y-%m-%d")
        end = datetime.strptime(end_date,"%Y-%m-%d")
        print dst_dataset+"."+ dst_table
        for it in range((end-start).days):
            single_date = start+timedelta(it)
            print str(single_date)[:10]
            partition = str(single_date)[:10]
            print "date="+partition
            partition_short = partition.replace("-","")
            sql2=self.sql.replace("{datePartition}",partition).replace("{project}",hook.project_id)
            print "sql="+sql2
            while True:
                try:
                    print "Run the job:"
                    job = hook.write_to_table(sql2,dst_dataset, dst_table+"$"+partition_short,use_legacy_sql=False, write_disposition='WRITE_TRUNCATE')
                    jobs.append(job)
                    print "Run this: "+str(job)+"  "+str(len(jobs))
                    break
                except JobInsertException:
                    print "Problem with the job, waiting..."
                    time.sleep(60)
                    print "try again..."
                    pass



            while len(jobs) >= 30:
                print "WAIT="+str(len(jobs))
                time.sleep(60)
                print "WAITED, resumed execution"
                jobs = self.clean_jobs(jobs,hook)

        while len(jobs) >0:
            for it in jobs:
                print "JOB remaining="+str(it)
            print "WAIT STR= "+str(len(jobs))
            time.sleep(60)
            jobs = self.clean_jobs(jobs,hook)
        print "Jobs are all executed"


class StreamByDatePartitionListOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary
    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'destination_table',
                       )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 partition_list_sql,
                 destination_table,
                 #start_date,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 schema_out = [],
                 *args,
                 **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        #self.start_date222=start_date
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.partition_list_sql = partition_list_sql
        self.schema_out = schema_out
        super(StreamByDatePartitionListOperator, self).__init__(*args, **kwargs)

    def clean_jobs(self,jobs):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        output = []
        for job in jobs:
            status=hook.get_status(job)
            print "STATUS="+str(status)+"  "+str(job)+"  "+str(len(jobs))
            if not status:
                output.append(job)
        print("Running jobs="+str(len(output)))
        return output


    def execute(self, context):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        print "hook.project_id="+hook.project_id
        jobs=[]
        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]
        print "::::::::::::::::::::::::::::::::::::::Schema_out::::::::::::::::::::::::::::::::::::::::::::::::::::::"
        print self.schema_out
        hook.create_table_with_partition(
            dst_dataset,
            dst_table,self.schema_out)
        print dst_dataset+"."+ dst_table
        partition_list_sql2 = self.partition_list_sql.replace("{project}",hook.project_id)
        print "partition_list_sql2="+partition_list_sql2
        job_id = hook.execute_query(partition_list_sql2, use_legacy_sql=False)
        partition_list =  hook.fetch(job_id)
        if len(partition_list)==0:
            print "Partition list is empty, skipping the step.."
            return
        for partition_record in partition_list:
            print partition_record
            partition = str(partition_record['partner_load_date'])[:10]
            print "date="+partition
            partition_short = partition.replace("-","")
            sql2=self.sql.replace("{datePartition}",partition).replace("{project}",hook.project_id)
            print "sql2="+sql2
            job = hook.write_to_table(sql2,dst_dataset, dst_table+"$"+partition_short,use_legacy_sql=False, write_disposition='WRITE_TRUNCATE')
            jobs.append(job)
            print str(job)+"  "+str(len(jobs))
            while len(jobs) >= 30:
                print "WAIT="+str(len(jobs))
                time.sleep(60)
                jobs = self.clean_jobs(jobs)

        while len(jobs) >0:
            for it in jobs:
                print "JOB remaining="+str(it)
            print "WAIT STR= "+str(len(jobs))
            time.sleep(60)
            jobs = self.clean_jobs(jobs)

class ArchiveByDateOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary
    """
    ui_color = '#33FFEC'
    template_fields = ('sql',
                       'destination_table',
                       )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 destination_table,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 schema_out = [],
                 *args,
                 **kwargs):
        self.sql = sql
        self.destination_table = destination_table
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.schema_out = schema_out
        super(ArchiveByDateOperator, self).__init__(*args, **kwargs)

    def clean_jobs(self,jobs):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        output = []
        for job in jobs:
            print str(job)+"  "+str(len(job))
            status=hook.get_status(job)
            #print "STATUS="+str(status)
            if not status:
                output.append(job)
        print("Running jobs="+str(len(output)))
        return output

    def execute(self, context):
        hook = BigQueryHookAR(bigquery_conn_id=self.bigquery_conn_id)
        jobs = []
        #getting dataset name from destination_table
        dst_table_array = self.destination_table.split('.')
        dst_table = dst_table_array[len(dst_table_array) - 1]
        dst_dataset = dst_table_array[len(dst_table_array) - 2]
        hook.create_table_with_partition(
            dst_dataset,
            dst_table,self.schema_out)
        for it in range(0,365):
            print "day="+str(it)
            partition_short="1"+'{:03d}'.format(it)+"0101"
            sql2 = self.sql.replace("{daySinceFirstStream}",str(it)).replace("{project}",hook.project_id)
            job_id=hook.write_to_table(sql2, dst_dataset, dst_table+"$"+partition_short,use_legacy_sql=False, write_disposition='WRITE_TRUNCATE')
            jobs.append(job_id)
            print str(job_id)+"  "+str(len(job_id))
            while len(jobs) >= 30:
                time.sleep(30)
                jobs = self.clean_jobs(jobs)

        while len(jobs) >0:
            time.sleep(30)
            jobs = self.clean_jobs(jobs)

class BigQueryPlugin(AirflowPlugin):
    name = "Archive AirFlow Plugin"
    operators = [
                 CreateReleaseOperator,
                 StreamByDateOperator,
                 StreamByDatePartitionListOperator,
                 ArchiveByDateOperator
                 ]

