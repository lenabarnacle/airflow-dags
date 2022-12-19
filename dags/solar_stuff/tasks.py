from datetime import datetime
import requests
from typing import List

import pandas as pd
from sqlalchemy import ARRAY, JSON
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import provide_session, NEW_SESSION

from dwh_resources import get_postgres_engine
from callbacks import on_failure_callback

doc_md = """
Документацию к API можно прочитать здесь: https://solar-staff.com/api/docs/#5fad145d14
"""

dag = DAG(
    "SolarSuffTasks",
    description="Получения заданий из SolarSuff",
    doc_md=doc_md,
    schedule_interval="@daily",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Brovko.NS",
        "on_failure_callback": on_failure_callback,
        "email": ["nikita.br@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def extract() -> List[dict]:
    """
    Извлечение данных из API Solar-Stuff.
    Документацию к API можно прочитать здесь: https://solar-staff.com/api/docs/#5fad145d14
    1. Извлекаем данные по всем задачам
    2. Находим параметр пагинации
    3. Получаем все страницы исходя из параметров пагинации
    """

    connection = BaseHook.get_connection("SolarStuffJWT")
    json_auth = connection.extra_dejson

    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {json_auth['token']}",
    }

    params = {
        "size": 500,
    }

    result = []

    response = requests.get(
        "http://solar-staff.com/api/customer/tasks", headers=headers, params=params
    )
    json = response.json()
    result += json["items"]

    pagination = json["pagination"]

    for page in range(pagination["page"] + 1, pagination["pages"] + 1):
        params["page"] = page
        response = requests.get(
            "http://solar-staff.com/api/customer/tasks", headers=headers, params=params
        )
        result += response.json()["items"]

    return result


def transform(data: List[dict]) -> pd.DataFrame:
    """
    Обрабатываем полученный из API Solar-Stuff JSON для загрузки в DWH

    Вход:
    data - данные из Solar Suff

    Выход:
    Обработанные данные из Solar Stuff
    """
    df = pd.json_normalize(data)

    return df


def load(df: pd.DataFrame):
    """
    Загружаем данные по задачам из Solar-Stuff в DWH

    Вход:
    df - обработанные данные из Solar Stuff
    """
    sql_conn = get_postgres_engine("DWHPostgreSQL_global_child")
    df.to_sql(
        name="tmp_solar_stuff_tasks",
        schema="sa",
        if_exists="replace",
        con=sql_conn,
        index=False,
        chunksize=1024,
        method="multi",
        dtype={
            "attributes": ARRAY(JSON),
            "messages": ARRAY(JSON),
            "files": ARRAY(JSON),
            "links": ARRAY(JSON),
            "actions": ARRAY(JSON),
        },
    )

    columns_str = ",".join(map(lambda col: f'"{col}"', df.columns))

    with Session(sql_conn) as session:
        session.execute("DELETE FROM sa.solar_stuff_tasks")
        session.execute(
            f"""
            INSERT INTO sa.solar_stuff_tasks
            ({columns_str})
            SELECT {columns_str} FROM sa.tmp_solar_stuff_tasks
            """
        )
        session.execute("DROP TABLE sa.tmp_solar_stuff_tasks")
        session.commit()


def __main__():
    """
    Классический ETL
    """
    data = extract()
    df = transform(data)
    load(df)


task = PythonOperator(task_id="etl", python_callable=__main__, dag=dag)

task
