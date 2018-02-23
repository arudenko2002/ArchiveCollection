from __future__ import print_function
import airflow
import logging
import sys
import pytz

from os import path

from datetime import datetime, timedelta

from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
#from airflow.models import Variable

# setting start date to some value in the past
# we will not be using it, so it's only for consistency of
# DAG configuration
#start_date = datetime(2017, 7, 12, 13, 0, 0, tzinfo=pytz.utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': None,
    'email': ['alexey.rudenko2002@umusic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# declare DAG
dagtest = DAG('test_bash_operator'
          ,description='Tests bash,operator,email cases'
          ,start_date=datetime(2017, 11, 03, 0, 10, 0)
          #,start_date=datetime.now()+timedelta(hours=-2)
          ,schedule_interval = "*/5 * * * *"
          ,default_args=default_args)
dagtest.catchup = False
dagtest.catchup_by_default=False

t11 = BashOperator(
    task_id='catchup_control',
    bash_command="echo AAAAAAAAAAAAA BBBBBBBBBB CCCCCCCCCC "+str(datetime.now()),
    dag=dagtest)

