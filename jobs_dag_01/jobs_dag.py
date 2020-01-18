import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

concurrency = 2
catchup = False

config = {
    'dag_id_1': {'schedule_interval': timedelta(minutes=45), 'start_date': datetime(2020, 1, 18), 'max_active_runs': 2},

    'dag_id_2': {'schedule_interval': timedelta(minutes=40), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 3},
    'dag_id_3': {'schedule_interval': timedelta(minutes=55), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 1},
    'dag_id_20': {'schedule_interval': timedelta(minutes=35), 'start_date': datetime(2020, 1, 18), 'max_active_runs': 1}
}

for dict in config:
    args = {
        'owner': 'airflow',
        'start_date': config[dict]['start_date'],
        'max_active_runs': config[dict]['max_active_runs'],
        'dagrun_timeout': timedelta(minutes=10),
        'concurrency': concurrency,
        'catchup': catchup
    }
    with DAG(dag_id=dict, default_args=args, schedule_interval=config[dict]['schedule_interval']) as dag:
        dop0 = DummyOperator(task_id='dummy-task-' + dict)
        dop1 = BashOperator(task_id='dummy-sub-task-' + dict, bash_command='echo `date`')
        dop1.set_upstream(dop0)

    if dag:
        globals()[dict] = dag
else:
    print("Finished")

""" test 2 """
