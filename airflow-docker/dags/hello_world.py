from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 
 
def print_hello(): 
    print("Hello from Airflow 3.0!") 
    return "success" 
 
default_args = { 
    'owner': 'beginner', 
    'depends_on_past': False, 
    'start_date': datetime(2024, 1, 1), 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
} 
 
dag = DAG( 
    'hello_world_dag', 
    default_args=default_args, 
    description='A simple hello world DAG', 
    schedule=timedelta(days=1), 
    catchup=False, 
    tags=['example', 'beginner'], 
) 
 
# Task 1: Bash command 
bash_task = BashOperator( 
    task_id='say_hello_bash', 
    bash_command='echo "Hello World from Bash!"', 
    dag=dag, 
) 
 
# Task 2: Python function 
python_task = PythonOperator( 
    task_id='say_hello_python', 
    python_callable=print_hello, 
    dag=dag, 
) 
 
# Set task dependencies 
bash_task >> python_task 
