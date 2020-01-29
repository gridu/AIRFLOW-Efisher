import airflow
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
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


def check_table_exist(**kwargs):
    table_exist = bool(random.getrandbits(1))

    if (table_exist == True):
        kwargs['ti'].xcom_push(table_exist=True)
        return 'skip_table_creation'
    else:
        kwargs['ti'].xcom_push(table_exist=False)
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

        dop0 = PythonOperator(task_id='python-task-' + dict,
                              provide_context=True,
                              op_kwargs={'database': database, 'table': config[dict]['table_name']},
                              python_callable=print_to_log
                              )

        dop01 = BranchPythonOperator(task_id='python-task-' + dict,
                               provide_context=True,
                               python_callable=check_table_exist()
                               )

        dop1 = DummyOperator(task_id='insert-new-row-' + dict)
        dop1.set_upstream(dop0)

        dop2 = DummyOperator(task_id='query-the-table-' + dict)
        dop2.set_upstream(dop1)

    if dag:
        globals()[dict] = dag
    else:
        print("Finished")

""" test 2 """
