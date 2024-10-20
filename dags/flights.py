from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from plugins.etl.extract.get_flights import get_flights
from plugins.etl.transform.transform import transform_data
from plugins.etl.load.load import load_to_redshift

from dotenv import load_dotenv

load_dotenv()


default_args = {
    "owner": "guidocaru",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "flights_dag",
    default_args=default_args,
    description="DAG for flights data",
    schedule_interval=timedelta(days=1),
    catchup=False,
)


t1 = PythonOperator(
    task_id="extract",
    python_callable=get_flights,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id="load",
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3
