from datetime import datetime
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG('data_pipeline', default_args=default_args, start_date=airflow.utils.dates.days_ago(1), schedule_interval="0 0 * * *")

def fetch_data_task():
    db_type = Variable.get("db_type")
    user = Variable.get("query")
    password = Variable.get("password")
    connection_string = Variable.get("connection_string")
    query = Variable.get("query")
    data = {
        "db_type": db_type,
        "connection": {
            "username": user,
            "password": password,
            "conn_string": connection_string,
        },
        "query": query,
    }

    response = requests.post(url=Variable.get("service_url"), json=data)

    return response

fetch_data_operator = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data_task,
    dag=dag,
)

def zip_data_task(**kwargs):
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_data_task')
    response = requests.post(url=Variable.get("service_url"), json=fetched_data)
    return response

zip_data_operator = PythonOperator(
    task_id='zip_data_task',
    python_callable=zip_data_task,
    provide_context=True,
    dag=dag,
)

def get_to_minio_task(**kwargs):
    ti = kwargs['ti']
    zip_response = ti.xcom_pull(task_ids='zip_data_task')
    zipped_data = zip_response.content

    minio_response = requests.post(url=Variable.get("service_url"), files={'file': zipped_data})
    return minio_response

get_to_minio_operator = PythonOperator(
    task_id='get_to_minio_task',
    python_callable=get_to_minio_task,
    provide_context=True,
    dag=dag,
)

fetch_data_operator >> zip_data_operator >> get_to_minio_operator