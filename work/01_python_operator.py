from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'user_id',
    'provide_context': True,
    'email': ['my@gmail.ru'],
    'email_on_failure': True,
    'start_date': datetime(2024, 3, 4),
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}


def my_python_func_args(num: int):
    result = num + 10
    print(f"result={result}")


def my_python_func_kwargs(num: int):
    result = num + 10
    print(f"result={result}")


with DAG(dag_id="01_practice_user_id",
         default_args=default_args,
         schedule_interval=None,
         tags=["user_id", "01_practice"]) as dag:

    start_op = EmptyOperator(task_id="start")

    my_name = BashOperator(
        task_id="my_name",
        bash_command="echo Andrey"
    )

    python_func_args = PythonOperator(
        task_id="python_func_args",
        python_callable=my_python_func_args,
        op_args=[20]
    )

    python_func_kwargs = PythonOperator(
        task_id="python_func_kwargs",
        python_callable=my_python_func_kwargs,
        op_kwargs={"num": 40}
    )

    stop_op = EmptyOperator(task_id="stop")

    start_op >> my_name >> python_func_args >> python_func_kwargs >> stop_op
