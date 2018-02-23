from __future__ import print_function
import airflow
import logging
import sys
import pytz
import common_operators

from os import path

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import WaitGCSOperator


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

#"swift_alerts/trackHistoryAPI_2018-01-25/track_history_2018-01-25.csv-00010-of-00026"
bucket="umg-dev"
prefix = "swift_alerts/trackHistoryAPI_2018-01-25/"
#filename = "track_history_2018-01-25.csv-00020-of-0026"
filename = "track_history_2018-01-25"

dagvelocitydaily2 = DAG('velocity_test_daily'
                       ,description='Testing'
                       ,start_date=datetime(2017, 12, 14, 0, 0, 0)
                       ,schedule_interval = "30 15 * * *"
                       #,schedule_interval = "None"
                       ,default_args=default_args)
dagvelocitydaily2.catchup=False

#from airflow.operators import WaitGCSOperator
GCS_Files = WaitGCSOperator(
    task_id='GCS_Files',
    bucket=bucket,
    prefix=prefix,
    number="20",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dagvelocitydaily2
)
