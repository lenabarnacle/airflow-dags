from airflow.hooks.base import BaseHook

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
