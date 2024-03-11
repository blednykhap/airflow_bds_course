from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def select():
    psql_hook = PostgresHook()
    ps_result = psql_hook.get_records(f"select max(bank_account) mx from final_practice where name = 'user_id'")
    str_result = ps_result[0][0]
    print(f'max bank_account = {ps_result}')
    if int(str_result) % 2 == 0:
        print('even')
    else:
        print('odd')


with DAG(
    dag_id='09_practice_user_id_2',
    schedule='35 12 * * 1,4',
    start_date=datetime(2024, 2, 15),
    tags=['09_practice', 'user_id']
) as dag:

    ets = ExternalTaskSensor(
        task_id='ets',
        external_dag_id='09_practice_user_id',
        external_task_id='x_ps'
    )

    call_hook = PythonOperator(
        task_id='call_hook',
        python_callable=select
    )

