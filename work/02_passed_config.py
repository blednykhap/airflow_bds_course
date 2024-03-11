from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def print_function(**kwargs) -> None:

    config = kwargs['dag_run'].conf

    if config is not None:
        num = config['number']
        result = int(num) + 20
        print(result)


with DAG(
    dag_id='02_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 4),
    tags=['02_practice', 'user_id']
) as dag:

    start = EmptyOperator(task_id='start')

    some_function = PythonOperator(
        task_id='pass_param',
        python_callable=print_function
    )

    stop = EmptyOperator(task_id='stop')

    start >> some_function >> stop

