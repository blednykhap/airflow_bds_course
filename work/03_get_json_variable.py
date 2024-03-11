from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import json

my_args = json.loads(Variable.get('args_user_id'))

def print_args():
    print(f'my_args: {my_args}')

with DAG(
    dag_id='03_practice_user_id',
    default_args=my_args,
    tags=['03_practice', 'user_id']
) as dag:

    py_op = PythonOperator(
        task_id="some_com",
        python_callable=print_args
    )
    start_up = EmptyOperator(task_id="start")
    stop_op = EmptyOperator(task_id="stop")

    start_up >> py_op >> stop_op
