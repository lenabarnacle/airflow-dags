from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from dwh_resources import get_google_sheets_client, get_postgres_engine

dag = DAG(
    "pnl_update",
    description="Загрузка данных в гугл таблицы финансовой модели",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Brovko.NS",
        "email": ["nikita.br@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def extract() -> pd.DataFrame:
    """
    Загрузить данные по онлайн когортам из DWH
    """
    engine = get_postgres_engine("DWHPostgreSQL")

    df = pd.read_sql_query(
        """
        SELECT brand, first_mon, purch_mon, revenue
        FROM dm.rep_ozon_by_brand_for_report_correct
        WHERE first_mon >= '2020-07-01'
        and purch_mon < now() - interval '1' month
        AND marketplace = 'OZON+WB'
        AND brand = '1.Все бренды';
        """,
        engine,
    )
    engine.dispose()

    return df


def transform(df_all_brands: pd.DataFrame) -> pd.DataFrame:
    """
    Трансформация данных в вид нужный для загрузки в гугл таблицу
    Вход:
    df_all_brands - необработанные данные для загрузки
    Выход:
    Обработанные данные для загрузки
    """

    df_all_brands["revenue"] = (
        df_all_brands["revenue"].astype(str).str.replace(".", ",")
    )
    df_all_brands_pivot = df_all_brands.pivot_table(
        index="first_mon", columns="purch_mon", values="revenue", aggfunc="sum"
    )

    return df_all_brands_pivot.fillna("")


def load(df: pd.DataFrame):
    """
    Загрузка данных в гугл таблицу
    Вход:
    df - данные готовы к загрузки
    """

    info_for_load = Variable.get(key="PNLToGoogleSheets", deserialize_json=True)
    gc = get_google_sheets_client("GoogleServiceAccount")
    sh = gc.open_by_key(info_for_load["table_id"])

    wks_write = sh.worksheet_by_title(info_for_load["workseet_name"])
    wks_write.set_dataframe(
        df, info_for_load["start_cell_id"], encoding="utf-8", fit=False, copy_head=False
    )


def __main__():
    """
    Функция для извлечения, загрузки и обработки данных по кагортам
    1. Извлекаем данные
    2. Обрабатываем
    3. Загружаем
    """
    df = extract()
    df = transform(df)
    load(df)


etl = PythonOperator(task_id="etl", python_callable=__main__, dag=dag)

etl
