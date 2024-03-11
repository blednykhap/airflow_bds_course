from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

filename = 'test_user_id.txt'

with DAG(
    dag_id='06_practice_user_id',
    schedule_interval=None,
    start_date=datetime(2024, 3, 5),
    tags=['06_practice', 'user_id']
) as dag:

    create_file = BashOperator(
        task_id='create_file',
        bash_command=f'touch /opt/airflow/config/{filename}'
    )

    list_files = BashOperator(
        task_id='list_files',
        bash_command=f'ls /opt/airflow/config/'
    )

    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=f'/opt/airflow/config/{filename}',
        poke_interval=10,
        timeout=180
    )

    create_file >> list_files >> file_sensor_task

