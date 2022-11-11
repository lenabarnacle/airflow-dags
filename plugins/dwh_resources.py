import json

from airflow.hooks.base import BaseHook

import boto3
from botocore import client
from botocore.config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def get_api_key_airtable(connection_id: str) -> str:
    """
    Возвращает ключ для авторизации в API Airtable
    Вход:
    connection_id (str) - идентификатор подключения
    Выход:
    str - ключ для API авторизации
    """
    connection = BaseHook.get_connection(connection_id)
    return connection.password


def get_postgres_connection_string(connection_id: str) -> str:
    """
    Возвращает строку подключения к postgres для указанного connection_id
    Вход:
    connection_id (str) - идентификатор подключения
    Выход:
    str - строка подключения
    Исключения:
    Exception: Тип подключения не postgres
    """

    connection = BaseHook.get_connection(connection_id)
    if connection.conn_type != "postgres":
        raise Exception("Тип подключения не postgres")
    return f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}?application_name=airflow_dwh"


def get_postgres_engine(connection_id: str) -> Engine:
    """
    Возвращает созданное соединение с postgresql для указанного connection_id
    Вход:
    connection_id (str) - идентификатор подключения
    Выход:
    Engine - соединение с postgres
    """
    engine = create_engine(
        get_postgres_connection_string(connection_id),
        connect_args={
            "sslmode": "verify-full",
            "sslrootcert": "/opt/airflow/plugins/postgresql.crt",
        },
    )

    return engine


def get_s3_client(connection_id: str) -> client.BaseClient:
    """
    Подготовка клиента S3
    """

    connection = BaseHook.get_connection(connection_id)
    if connection.conn_type != "aws":
        raise Exception("Тип подключения не Amazon S3")

    my_config = Config(
        region_name="ru-central1",
    )

    return boto3.client(
        "s3",
        config=my_config,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        endpoint_url=json.loads(connection.extra)["endpoint_url"],
    )
