from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 11),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "basic_airflow_dag",
    default_args=default_args,
    description="A basic Airflow DAG",
    schedule_interval=timedelta(days=1),
)


# Define some example tasks
def task_1():
    print("Executing Task 1")


def task_2():
    now = pendulum.now("Europe/Paris")
    print(now)
    print("Executing Task 2")


def task_3():
    print("Executing Task 3")


# Create PythonOperator tasks
t1 = PythonOperator(
    task_id="task_1",
    python_callable=task_1,
    dag=dag,
)

t2 = PythonOperator(
    task_id="task_2",
    python_callable=task_2,
    dag=dag,
)

t3 = PythonOperator(
    task_id="task_3",
    python_callable=task_3,
    dag=dag,
)

# Set task dependencies
t1 >> [t2, t3]
