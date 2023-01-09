from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

from callbacks import on_failure_callback
from dwh_resources import get_postgres_connection_string, get_api_key_airtable

dag = DAG(
    "airtable_payments_register_retail",
    description="Выгрузка данных из реестра платежей Retail Airtable",
    schedule_interval="@daily",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["Airtable", "loaders"],
    default_args={
        "on_failure_callback": on_failure_callback,
        "owner": "Brovko.NS",
        "email": ["nikita.br@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def get_environment():
    """
    Функция для получения переменных окружения для корректной работы контейнера
    """
    db_url = get_postgres_connection_string("DWHPostgreSQL_global_child")

    return {
        "LOG_LEVEL": "INFO",
        "DB_URL": db_url,
        "DB_TABLE_NAME": "airtable_payments_register_retail",
        "DB_SCHEMA_NAME": "sa",
        "AIRTABLE_TOKEN": get_api_key_airtable("Airtable"),
        "AIRTABLE_BASE_ID": "appJcsehQlbs4c8L2",
        "AIRTABLE_TABLE_ID": "tblNhXl6pNEg8R0qn",
    }


task = DockerOperator(
    dag=dag,
    task_id="MainTask",
    docker_conn_id="CarelyRegistry",
    image="registry.gitlab.com/urbnvape/loaders/airtable",
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    environment=get_environment(),
)

task
