import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

concurrency = 2
catchup = False

config = {
    'dag_id_1': {'schedule_interval': timedelta(minutes=15), 'start_date': datetime(2020, 1, 16), 'max_active_runs': 1},

    'dag_id_2': {'schedule_interval': timedelta(minutes=20), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 1},
    'dag_id_3': {'schedule_interval': timedelta(minutes=25), 'start_date': airflow.utils.dates.days_ago(2),
                 'max_active_runs': 1},

    'dag_id_20': {'schedule_interval': timedelta(minutes=15), 'start_date': datetime(2020, 1, 16), 'max_active_runs': 1}
}

for dict in config:
    with DAG(dag_id=dict, schedule_interval=config[dict]['schedule_interval'], start_date=config[dict]['start_date'], max_active_runs=config[dict]['max_active_runs'], dagrun_timeout=timedelta(minutes=10), concurrency=concurrency, catchup=catchup) as dag:
        dop0 = DummyOperator(task_id='dummy-task-'.join(dict))
        dop1 = BashOperator(task_id='dummy-sub-task-'.join(dict), bash_command='echo `date`')
        dop1.set_upstream(dop0)
else:
    print ("Finished")

""" test 2 """
