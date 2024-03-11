from __future__ import annotations

from datetime import datetime
from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='04_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 5),
    tags=['04_practice', 'user_id']
) as dag:

    start_up = EmptyOperator(task_id="start")

    def calculate_average():

        numbers = Variable.get('numbers_user_id')
        my_avg = eval(f"sum({numbers}) / len({numbers})")
        Variable.set('average_user_id', my_avg)
        print("my_avg")

    @task
    def calculate_average_task():
        calculate_average()

    stop_op = EmptyOperator(task_id="stop")

    calc = calculate_average_task()

    start_up >> calc >> stop_op



