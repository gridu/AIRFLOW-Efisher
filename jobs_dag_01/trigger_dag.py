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
    'triggered_dag_id_1': {'schedule_interval': timedelta(minutes=45),
                           'start_date': datetime(2020, 2, 3),
                           'max_active_runs': 1
                           }
}


for dict in config:
    args = {
        'owner': 'airflow',
        'start_date': config[dict]['start_date'],
        'dagrun_timeout': timedelta(minutes=10),
        'retries': 3,
        'retry_delay': datetime.timedelta(hours=5),
        'poke_interval': 30
    }

    with DAG(dag_id=dict, default_args=args, schedule_interval=config[dict]['schedule_interval'],
         start_date =config[dict]['start_date'],
         concurrency=concurrency,
         max_active_runs =config[dict]['max_active_runs']) as dag:

        sensor000 = FileSensor(task_id="file_sensor_task",
                                poke_interval=default_args['poke_interval'],
                                fs_conn_id="fs_default",
                                filepath="/tmp/trigger_it")


    if dag:
        globals()[dict] = dag
    else:
        print("Finished")