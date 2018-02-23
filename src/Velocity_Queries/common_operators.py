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

class WaitHook(GoogleCloudBaseHook, DbApiHook):

    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.
    """
    conn_name_attr = 'bigquery_conn_id'
    project_id='umg-dev'
    json_key_file='/Users/rudenka/Downloads/UMGDevCreds.json'

    def __init__(self,
                 bigquery_conn_id='bigquery_default'):
        super(WaitHook, self).__init__(
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
                                             use_legacy_sql=use_legacy_sql)
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
            #self.client().create_table(dataset,table,schema=None, expiration_time=None,time_partitioning=True)
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
        print "JOB="+job_record["id"].split(":")[1]
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

class WaitQueryOperator(BaseOperator):
    """
    Incrementally loads data from one table to another, repartitioning if necessary
    """
    ui_color = '#33FFEC'
    template_fields = (
        'sql',
                       )
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 bigquery_conn_id='bigquery_default',
                 use_legacy_sql=False,
                 *args,
                 **kwargs):
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql

        super(WaitQueryOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = WaitHook(bigquery_conn_id=self.bigquery_conn_id)
        print "AR hook.project_id="+hook.project_id
        counter=0
        while True:
            counter=counter+1
            if counter>30:
                print "AR The BQ result of the previous step is not obtained.  Timeouting.."
                return
            job_id = hook.execute_query(self.sql, use_legacy_sql=False)
            print("sql="+self.sql)
            partition_list =  hook.fetch(job_id)
            if len(partition_list)>0:
                print "# of records="+str(len(partition_list))
                print "AR The result is ready, proceed to the next step.."
                return
            else:
                print "AR Waiting..."
                time.sleep(120)

class BigQueryPlugin(AirflowPlugin):
    name = "Common Plugin"
    operators = [
        WaitQueryOperator,
    ]

