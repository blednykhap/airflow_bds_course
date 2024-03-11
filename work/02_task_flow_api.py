from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id='02_practice_user_id_tfapi',
    schedule_interval=None,
    start_date=datetime(2024, 3, 4),
    tags=['02_practice', 'user_id'])
def print_numb():

    @task
    def some_function(**kwargs):
        config = kwargs['dag_run'].conf
        print(f'Print from function: {config}')

        if config is not None:
            num = config['number']
            result = int(num) + 20
            print(result)

    start_up = EmptyOperator(task_id='start_up')

    test = some_function()

    stop_op = EmptyOperator(task_id='stop')

    start_up >> test >> stop_op

print_numb()