import airflow
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

concurrency = 2
catchup = False
database = "our_test_db"

config = {
    'dag_id_1': {'schedule_interval': "@once", 'start_date': datetime(2020, 2, 3), 'max_active_runs': 1,
                 "table_name": "table_num_1"},
    'dag_id_2': {'schedule_interval': timedelta(minutes=40), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 1, "table_name": "table_num_2"},
    'dag_id_3': {'schedule_interval': timedelta(minutes=55), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 1, "table_name": "table_num_3"}
}


def print_to_log(ti, **kwargs):
    dag_id = ti.dag_id
    database = kwargs['database']
    table = kwargs['table']
    print("[print_to_log] %s start processing tables in database: %s.%s" % (dag_id, database, table))
    return "[print_to_log] end"


def check_table_exist(ti, **kwargs):
    table_exist = bool(random.getrandbits(1))


    if table_exist == True:
        ti.xcom_push(key='table_exist', value=True)
        print('Table exists')

    else:
        ti.xcom_push(key='table_exist', value=False)
        print('Table doesnt exists')


def create_or_not_table(ti, **kwargs):


    xcom_value = bool(ti.xcom_pull(key='table_exist'))

    if xcom_value == True:
        print('This is DAG {}, creating table')
        return 'skip_table_creation'
    else:
        print('This is DAG {}, skip creating table')
        return 'create_table'



for dict in config:
    args = {
        'owner': 'airflow',
        'start_date': config[dict]['start_date'],
        'dagrun_timeout': timedelta(minutes=10),
        'concurrency': concurrency,
        'catchup': catchup
    }
    with DAG(dag_id=dict, default_args=args, schedule_interval=config[dict]['schedule_interval'],
             start_date=config[dict]['start_date'],
             concurrency=concurrency,
             max_active_runs=config[dict]['max_active_runs']) as dag:

        dop00 = PythonOperator(task_id='python-task-' + dict,
                              provide_context=True,
                              op_kwargs={'database': database, 'table': config[dict]['table_name']},
                              python_callable=print_to_log
                              )
        dop001 = BashOperator(task_id='bash-sub-task-' + dict, bash_command='echo "Running under user: ${USER}"')
        dop001.set_upstream(dop00)

        dop01 = PythonOperator(task_id='check-table-task-' + dict,
                               provide_context=True,
                               python_callable=check_table_exist
                               )
        dop01.set_upstream(dop001)


        dop02 = BranchPythonOperator(task_id='create_or_not_table' + dict,
                               provide_context=True,
                               python_callable=create_or_not_table
                               )

        dop02.set_upstream(dop01)

        dop03 = DummyOperator(task_id='create_table')
        dop03.set_upstream(dop02)
        dop04 = DummyOperator(task_id='skip_table_creation')
        dop04.set_upstream(dop02)

        dop05 = DummyOperator(task_id='insert-new-row-' + dict, trigger_rule='none_failed')
        dop05.set_upstream([dop03, dop04])

        dop06 = DummyOperator(task_id='query-the-table-' + dict)
        dop06.set_upstream(dop05)

    if dag:
        globals()[dict] = dag
    else:
        print("Finished")

""" test 2 """
