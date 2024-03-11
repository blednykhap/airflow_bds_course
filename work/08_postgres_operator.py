from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id='08_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 6),
    tags=['08_practice', 'user_id']
) as dag:

    start_up = EmptyOperator(
        task_id='start'
    )

    postgres_insert = PostgresOperator(
        task_id='postgres_insert',
        sql="INSERT INTO students(id, name, create_date) VALUES(7, '{{ var.json.students.get('7') }}', '{{ ds_nodash }}')"
    )

    stop_op = EmptyOperator(
        task_id='stop'
    )

    start_up >> postgres_insert >> stop_op

