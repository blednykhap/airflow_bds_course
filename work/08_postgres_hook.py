from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def select():
    psql_hook = PostgresHook()
    ps_result = psql_hook.get_records(f'select create_date from students where id = 7')
    str_result = ps_result[0][0]
    dt_result = datetime.strptime(str_result, '%Y%m%d')
    print(dt_result)
    dt_result += relativedelta(years=1)
    print(dt_result)

with DAG(
    dag_id='08_practice_user_id_2',
    schedule_interval=None,
    start_date=datetime(2024, 3, 6),
    tags=['08_practice', 'user_id']
) as dag:

    start_up = EmptyOperator(
        task_id='start'
    )

    call_hook = PythonOperator(
        task_id='call_hook',
        python_callable=select
    )

    stop_op = EmptyOperator(
        task_id='stop'
    )

    start_up >> call_hook >> stop_op
