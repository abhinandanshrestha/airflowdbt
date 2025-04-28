from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

@task.branch
def check_odd(number: int):
    if number % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    dag_id='branching',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['example'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_or_skipped')
    
    odd_task = EmptyOperator(task_id='odd_task')
    even_task = EmptyOperator(task_id='even_task')
    
    branch = check_odd(2)
    
    # Wiring downstream tasks explicitly
    start >> branch
    branch >> odd_task
    branch >> even_task
    odd_task >> end
    even_task >> end