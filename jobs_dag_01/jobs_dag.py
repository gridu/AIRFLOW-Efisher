import airflow
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime, timedelta

concurrency = 2
catchup = False
database = "our_test_db"

config = {
    'dag_id_1': {'schedule_interval': timedelta(minutes=45), 'start_date': datetime(2020, 1, 28), 'max_active_runs': 2,
                 "table_name": "table_num_1"},
    'dag_id_2': {'schedule_interval': timedelta(minutes=40), 'start_date': airflow.utils.dates.days_ago(1),
                 'max_active_runs': 3, "table_name": "table_num_2"},
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
        ti.xcom_push(table_exist=True)

    else:
        ti.xcom_push(table_exist=False)


def create_or_not_table(ti, **kwargs):


    xcom_value = bool(ti.xcom_pull(table_exist))

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
        'max_active_runs': config[dict]['max_active_runs'],
        'dagrun_timeout': timedelta(minutes=10),
        'concurrency': concurrency,
        'catchup': catchup
    }
    with DAG(dag_id=dict, default_args=args, schedule_interval=config[dict]['schedule_interval']) as dag:

        dop00 = PythonOperator(task_id='python-task-' + dict,
                              provide_context=True,
                              op_kwargs={'database': database, 'table': config[dict]['table_name']},
                              python_callable=print_to_log
                              )

        dop01 = PythonOperator(task_id='check-table-task-' + dict,
                               provide_context=True,
                               python_callable=check_table_exist
                               )
        dop01.set_upstream(dop00)


        dop02 = BranchPythonOperator(task_id='create_or_not_table' + dict,
                               provide_context=True,
                               python_callable=create_or_not_table
                               )

        dop02.set_upstream(dop01)

        dop03 = DummyOperator(task_id='create_table')
        dop03.set_upstream(dop02)
        dop04 = DummyOperator(task_id='skip_table_creation')
        dop03.set_upstream(dop02)

        dop05 = DummyOperator(task_id='insert-new-row-' + dict)
        dop03.set_downstream(dp05)
        dop04.set_downstream(dp05)

        dop06 = DummyOperator(task_id='query-the-table-' + dict)
        dop06.set_upstream(dop05)

    if dag:
        globals()[dict] = dag
    else:
        print("Finished")

""" test 2 """
