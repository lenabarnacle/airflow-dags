from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.hooks.base import BaseHook

from callbacks import on_failure_callback

dag = DAG(
    "DDLBackUp",
    description="История изменений DDL схем в DWH",
    schedule_interval="@hourly",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["Administration DWH"],
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
    postgresql = BaseHook.get_connection("DWHPostgreSQL_global_child")
    gitlab = BaseHook.get_connection("GitLab")

    return {
        "PGDATABASE": postgresql.schema,
        "PGHOST": postgresql.host,
        "PGPORT": postgresql.port,
        "PGUSER": postgresql.login,
        "PGPASSWORD": postgresql.password,
        "GITLINK": f"https://{gitlab.login}:{gitlab.password}@gitlab.com/urbnvape/loaders/dwh_ddl_backup.git",
    }


task = DockerOperator(
    dag=dag,
    task_id="DDLBackUp",
    docker_conn_id="CarelyRegistry",
    image="registry.gitlab.com/urbnvape/loaders/dwh_ddl_backup_script",
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    environment=get_environment(),
)

task
