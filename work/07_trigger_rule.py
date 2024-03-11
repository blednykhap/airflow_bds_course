# https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

filepath = '/opt/airflow/config/'
test_filename = 'test_user_id.txt'
aim_filename = '07_practice_user_id.txt'

with DAG(
    dag_id='07_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 6),
    tags=['07_practice', 'user_id']
) as dag:

    create_file = BashOperator(
        task_id='create_file',
        bash_command=f'touch {filepath}{test_filename}'
    )

    check_file = FileSensor(
        task_id='check_file',
        filepath=f'{filepath}{aim_filename}',
        poke_interval=5,
        timeout=10,
        trigger_rule='one_success'
    )

    rename_file = BashOperator(
        task_id='rename_file',
        bash_command=f'mv {filepath}{test_filename} {filepath}{aim_filename}',
        trigger_rule='all_failed'
    )

    create_file >> check_file >> rename_file

