from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from datetime import datetime, timezone, timedelta
from pprint import pprint


concurrency = 4
catchup = False
trigger_path = Variable.get("trigger_path_var", default_var='/tmp/trigger_it')

config = {
    'dag_with_subdag_id_1': {'schedule_interval': timedelta(minutes=45),
                             'start_date': datetime(2020, 2, 3, 9, 0, 0, 0, tzinfo=timezone.utc),
                             'max_active_runs': 1
                             }
}


def get_date(execution_date,**kwargs):

    next_execution_date = execution_date
    print ("{}->{}".format(execution_date,next_execution_date))
    return next_execution_date

def load_subdag(parent_dag_name, child_dag_name, args):


    def print_result(ti, **kwargs):
        xcom_value = (ti.xcom_pull(key='all_done', dag_id='dag_id_1'))
        print('This is {} - We are done'.format(xcom_value))
        pprint('context: {}'.format(kwargs))
        print('ti: {}'.format(ti))
        return 'Whatever you return gets printed in the logs'


    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        catchup=False
        )

    with dag_subdag:
        t = DummyOperator(
            task_id='load_subdag_{0}'.format(child_dag_name),
            default_args=args,
            dag=dag_subdag,
        )

        external_check = ExternalTaskSensor(
            task_id='ext_check_task_{0}'.format(child_dag_name),
            external_dag_id='dag_id_1',
            external_task_id=None,
            execution_date_fn=get_date,
            mode="reschedule",
            default_args=args,
            allowed_states=['success'],
            dag=dag_subdag,
        )

        print_result = PythonOperator(task_id='print_result_task_{0}'.format(child_dag_name),
                                      provide_context=True,
                                      python_callable=print_result,
                                      default_args=args,
                                      dag=dag_subdag
                                      )

        trigger_off = BashOperator(task_id='trigger_off', bash_command='rm -f {}'.format(trigger_path))

        timestamp = '{{ ts_nodash }}'
        result_file = BashOperator(task_id='create_finished_timestamp', bash_command='touch /tmp/finished_#{}'.format(timestamp))

        t >> external_check >> print_result >> trigger_off >> result_file

    return dag_subdag
"""
for dict in config:
    args = {
        'owner': 'airflow',
        'start_date': config[dict]['start_date'],
        'dagrun_timeout': timedelta(minutes=10),
        'retries': 30,
        'retry_delay': timedelta(minutes=5),
        'poke_interval': 60
    }
    
    with DAG(dag_id=dict,
             default_args=args,
             schedule_interval=config[dict]['schedule_interval'],
             start_date=config[dict]['start_date'],
             concurrency=concurrency,
             catchup=False,
             max_active_runs=config[dict]['max_active_runs']) as dag:

        sub_dag = SubDagOperator(
            subdag=load_subdag('{0}'.format(dict), '{0}_subdag'.format(dict), args),
            task_id='{0}_subdag'.format(dict),
        )

    if dag:
        globals()[dict] = dag
    else:
        print("Finished")
"""