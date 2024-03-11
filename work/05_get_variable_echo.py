from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='05_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 5),
    tags=['05_practice', 'user_id']
) as dag:

    start_up = EmptyOperator(task_id="start")

    print_variable = BashOperator(
        task_id="print_var",
        bash_command=f'echo {Variable.get("average_user_id")}'
    )

    stop_op = EmptyOperator(task_id="stop")

    start_up >> print_variable >> stop_op



