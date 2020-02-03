import airflow
from airflow import DAG

from airflow.contrib.sensors import file_sensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta



concurrency = 2
catchup = False
database = "our_test_db"

config = {
    'triggered_dag_id_1': {'schedule_interval': timedelta(minutes=45), 'start_date': datetime(2020, 2, 3), 'max_active_runs': 1
    }


for dict in config:
    args = {
        'owner': 'airflow',
        'start_date': config[dict]['start_date'],
        'dagrun_timeout': timedelta(minutes=10),
        'concurrency': concurrency,
        'catchup': catchup
    }

    with DAG(dag_id = dict, default_args = args, schedule_interval = config[dict]['schedule_interval'],
         start_date = config[dict]['start_date'],
         concurrency= concurrency,
         max_active_runs = config[dict]['max_active_runs']) as dag:


    if dag:
        globals()[dict] = dag
    else:
        print("Finished")
