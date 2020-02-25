from airflow import DAG

from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable

from datetime import datetime, timezone, timedelta

from jobs_dag_01.its_sub_dag import load_subdag

trigger_path = Variable.get("trigger_path_var", default_var='/tmp/trigger_it')

concurrency = 4
catchup = False
database = "our_test_db"

config = {
    'triggered_dag_id_1': {'schedule_interval': timedelta(minutes=45),
                           'start_date': datetime(2020, 2, 3, 9, 0, 0, 0, tzinfo=timezone.utc),
                           'max_active_runs': 1
                           }
}

def get_date(execution_date,**kwargs):

    next_execution_date = execution_date
    print ("{}->{}".format(execution_date,next_execution_date))
    return next_execution_date

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
         start_date =config[dict]['start_date'],
         concurrency=concurrency,
         catchup = False,
         max_active_runs =config[dict]['max_active_runs']) as dag:



        sensor000 = FileSensor(task_id="file_sensor_task",
                                fs_conn_id="fs_default",
                                mode="reschedule",
                                filepath=trigger_path)

        trigger_on_000 = TriggerDagRunOperator(task_id="trigger_on", trigger_dag_id="dag_id_1", execution_date='{{ execution_date }}')

        sub_dag = SubDagOperator(
            subdag=load_subdag('{0}'.format(dict), '{0}_subdag'.format(dict), args),
            task_id='{0}_subdag'.format(dict),
        )

        sensor000 >> trigger_on_000 >>  sub_dag

        """
        trigger_off_000 = BashOperator(task_id='trigger_off', bash_command='rm -f {{ trigger_path }}')

        external_check = ExternalTaskSensor(
            task_id='check_dag_id_1',
            external_dag_id='dag_id_1',
            external_task_id=None,
            execution_date_fn=get_date,
            mode="reschedule",
            allowed_states=['success']
        )


        trigger_on_000.set_upstream(sensor000)
        external_check.set_upstream(trigger_on_000)
        trigger_off_000.set_upstream(external_check)
        """
    if dag:
        globals()[dict] = dag
    else:
        print("Finished")
