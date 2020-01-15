from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

config = {
    'dag_id_1': {'schedule_interval': timedelta(minutes=15), "start_date": datetime(2020, 01, 11)},
    'dag_id_2': {'schedule_interval': timedelta(minutes=20), "start_date": datetime(2020, 01, 11)},
    'dag_id_3': {'schedule_interval': timedelta(minutes=25), "start_date": datetime(2020, 01, 11)}
}
