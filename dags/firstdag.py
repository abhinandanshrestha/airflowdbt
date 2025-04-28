from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN = Variable.get("postgres_conn")

# Python function using TaskFlow @task decorator
@task
def say_hello_python():
    print("Hello from Python using @task! ", PG_CONN)

@task
def print_sql_statement():
    # Set up the PostgreSQL hook
    conn_id = 'airflowinternaldb'
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    # Get the connection object
    conn = postgres_hook.get_conn()
    # Create a cursor
    cursor = conn.cursor()
    
    # Execute the query
    cursor.execute("SELECT * FROM ab_user;")
    
    # Fetch and print results
    results = cursor.fetchall()
    print("Results:", results)

# Define a Python function
def my_python_function():
    print("Hello from Python!")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    dag_id='first_python_shell_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  # every minute
    catchup=False,
    tags=['example'],
) as dag:

    # Python task
    python_task = PythonOperator(
        task_id='run_python_code',
        python_callable=my_python_function
    )

    # Bash task
    shell_task = BashOperator(
        task_id='run_shell_command',
        bash_command='echo "Hello from Shell!"'
    )

    # Python task using decorator
    python_task_using_decorator = say_hello_python()

    # Task pipeline (ensure both python tasks run before shell_task)
    [python_task_using_decorator, python_task] >> shell_task >> print_sql_statement()
