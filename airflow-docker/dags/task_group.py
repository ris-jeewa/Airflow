from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id='task_group_example',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False)


task1 = BashOperator(
    task_id='task1',
    bash_command="sleep 5",
    dag=dag)
task2 = BashOperator(
    task_id='task1',
    bash_command="sleep 5",
    dag=dag)

task1 >> task2