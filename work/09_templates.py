# https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#

from datetime import datetime
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def push_bank_account(ti):
    amt = random.randint(0, 10000)
    ti.xcom_push(key='bank_account_user_id', value=amt)
    print(f'push_bank_account : bank_account_user_id : {amt}')

with DAG(
    dag_id='09_practice_user_id',
    schedule='35 12 * * 1,4',
    start_date=datetime(2024, 2, 15),
    tags=['09_practice', 'user_id']
) as dag:

    x_py = PythonOperator(
        task_id='x_py',
        python_callable=push_bank_account
    )

    x_ps = PostgresOperator(
        task_id='x_ps',
        sql="""
            INSERT INTO final_practice(id, name, bank_account, time_key) 
            VALUES(7, '{{ var.json.students.get('7') }}', '{{ ti.xcom_pull(key="bank_account_user_id") }}', 'macros.ds_add(ds, -7)')
        """
    )

    x_py >> x_ps

