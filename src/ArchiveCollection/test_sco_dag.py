from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

#import util

from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.bash_operator import BashOperator


def extract_from_source_db(**kwargs):
    logging.info("Extracting from source db ...")


def transform_data(**kwargs):
    logging.info("Transforming data ...")


def load_target_db(**kwargs):
    logging.info("Loading to target db ...")


# Create DAG
dag = DAG('skip_past'
          ,description='Tests bash,operator,email cases'
          ,start_date=datetime(2017, 11, 8, 23, 31, 00)
          #,start_date=datetime.now()+timedelta(minutes=-20)
          ,schedule_interval = "*/5 * * * *"
          #,default_args=default_args)
        )
dag.catchup=False

def get_num_active_dagruns(dag_id, conn_id='airflow_db'):
#     airflow_db = PostgresHook(postgres_conn_id=conn_id)
#     conn = airflow_db.get_conn()
#     cursor = conn.cursor()
#     sql = """
# select count(*)
# from public.dag_run
# where dag_id = '{dag_id}'
#   and state in ('running', 'queued', 'up_for_retry')
# """.format(dag_id=dag_id)
#     cursor.execute(sql)
#     num_active_dagruns = cursor.fetchone()[0]
    #return num_active_dagruns
    return 1

def is_latest_active_dagrun(**kwargs):
    """Ensure that there are no runs currently in progress and this is the most recent run."""
    logging.info("kwargs="+str(kwargs))
    num_active_dagruns = get_num_active_dagruns(kwargs['dag'].dag_id)
    logging.info("num_active_dagruns"+str(num_active_dagruns))
    # first, truncate the date to the schedule_interval granularity, then subtract the schedule_interval
    schedule_interval = kwargs['dag'].schedule_interval
    logging.info("schedule_interval="+str(schedule_interval));
    now_epoch = int(datetime.now().strftime('%s'))
    #sched=schedule_interval.total_seconds()
    sched=600
    now_epoch_truncated = now_epoch - (now_epoch % sched)
    logging.info("now_epoch_truncated="+str(now_epoch_truncated));
    expected_run_epoch = now_epoch_truncated - 600
    logging.info("expected_run_epoch="+str(expected_run_epoch));
    expected_run_execution_date = datetime.fromtimestamp(expected_run_epoch)
    logging.info(" expected_run_execution_date="+str( expected_run_execution_date));
    is_latest_dagrun = kwargs['execution_date'] == expected_run_execution_date
    logging.info("Is latest dagrun: " + str(is_latest_dagrun))
    logging.info("Num dag runs active: " + str(num_active_dagruns))
    # If return value is False, then all downstream tasks will be skipped
    is_latest_active_dagrun = (is_latest_dagrun and num_active_dagruns == 1)
    return is_latest_active_dagrun

def to_run_next(**kwargs):
    num_active_dagruns = get_num_active_dagruns(kwargs['dag'].dag_id)
    logging.info("id="+kwargs['dag'].dag_id)
    num_active_dagruns = 1
    logging.info("kwargs="+str(kwargs))
    schedule_interval=300
    now_epoch = int(datetime.now().strftime('%s'))

    logging.info("now_epoch: " + str(now_epoch)+"   "+str(datetime.fromtimestamp(now_epoch)))
    now_epoch_truncated = now_epoch - (now_epoch % schedule_interval)
    expected_run_epoch = now_epoch_truncated - 300
    logging.info("now_epoch_truncated: " + str(now_epoch_truncated)+"   "+str(datetime.fromtimestamp(now_epoch_truncated)))
    if now_epoch % schedule_interval <= 7:
        return True
    else:
        return False

    if now_epoch-expected_run_epoch>schedule_interval+7:
        return False
    else:
        return True

# Skip unnecessary executions
doc = """
Skip the subsequent tasks if
    a) the execution_date is in past
    b) there multiple dag runs are currently active
"""
start_task = ShortCircuitOperator(
    task_id='skip_check',
    #python_callable=is_latest_active_dagrun,
    python_callable=to_run_next,
    provide_context=True,
    depends_on_past=True,
    dag=dag
)
start_task.doc = doc

t11 = BashOperator(
    task_id='catchup_control',
    bash_command="echo AAAAAAAAAAAAA BBBBBBBBBB CCCCCCCCCC "+str(datetime.now()),
    dag=dag)

start_task >> t11


# Extract
doc = """Extract from source database"""
extract_task = PythonOperator(
    task_id='extract_from_db',
    python_callable=extract_from_source_db,
    provide_context=True
)
extract_task.doc = doc
start_task >> extract_task
#
#
# # Transform
# doc = """Transform data"""
# transform_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     provide_context=True
# )
# transform_task.doc = doc
# extract_task >> transform_task
#
#
# # Load
# doc = """Load target database"""
# load_task = PythonOperator(
#     task_id='load_target_db',
#     python_callable=load_target_db,
#     provide_context=True
# )
# load_task.doc = doc
# transform_task >> load_task