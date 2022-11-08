from datetime import datetime
from typing import List

import pandas as pd
import pytz
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import ARRAY, JSON, DateTime, String
from sqlalchemy.orm import Session
from transliterate import translit

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine, get_api_key_airtable


def columns_name_processing(columns_name: List[str]) -> List[str]:
    """
    В PostgreSQL есть ограничения по количеству символов в наименовании объектов: столбцов, таблиц, представлений процедур.
    Объект может именоваться 31 кирилическим символом или 63 латинскими.
    Чтобы создавать объекты с названиями аналогичными источнику я сделал транслитерацию.
    Если длина наименования столбца более 32 символов, то транслитирируем в английский, иначе ничего не делаем.
    По мимо этого функция pd.json_normalize при нормализации JSON отдает
    именования типа fields.ColName, поэтому fields. удаляется из наименования столбца.
    Вход:
    columns_name - список с именами столбцов, которые нужно обработать
    Выход:
    Список с обработанными именами столбцов
    """
    new_columns_name = []
    for column_name in columns_name:
        column_name = (
            column_name.replace("fields.", "").replace("(", "").replace(")", "")
        )
        if len(column_name) > 32:
            new_columns_name.append(translit(column_name, "ru", reversed=True))
        else:
            new_columns_name.append(column_name)

    return new_columns_name


def extract():
    """
    Получить данные о всех продуктах из Airtables.
    Данные отдаются до тех пор пока в вернувшимся в ответ на запрос JSON есть параметр offset
    """
    result = []

    url = "https://api.airtable.com/v0/applO9887XZHZL9Dq/Все продукты"
    params = {"api_key": get_api_key_airtable("Airtable"), "limit": 100}

    http_client = requests.session()

    r = http_client.get(url, params=params)
    print(r.status_code)
    while True:
        dict = r.json()
        result += dict["records"]
        try:
            params["offset"] = dict["offset"]
            r = http_client.get(url, params=params)
        except KeyError:
            # Есть ключа offset нет в получаемом JSON, значит забор данных необходимо остановить. Мы забрали всё что нужно
            return result


def transform(extract_data: List(dict)) -> pd.DataFrame:
    """
    1. Из полученного JSON убирается уровень fields
    2. Колонки приводятся к транслитиральным именованиям, так как наименование столбца в PostgreSQL это максимум 32 символа
    3. Добавляется столбец timestamp, который содержит дату загрузки
    Вход:
    extract_data - данные по таблицам из Airtables
    Выход:
    df - подготовленный к загрузке
    """
    df = pd.json_normalize(extract_data, max_level=1)
    df.columns = columns_name_processing(df.columns)
    df["timestamp"] = datetime.now(tz=pytz.utc)

    return df


def load(transform_data: pd.DataFrame):
    """
    Загрузка преобразованных данных в DWH
    1. Загружаем данные во временную таблицу
    2. Открываем транзакцию
    3. Удаляем все данные в основной таблицы
    4. Вставляем новые данные в основную таблицу
    5. Удаляем врменную таблицу
    6. Закрываем транзакцию
    Вход:
    transform_data - преобразованные данные
    """
    sql_conn = get_postgres_engine("DWHPostgreSQL")

    transform_data.to_sql(
        name="tmp_airtable_all_products",
        schema="sa",
        con=sql_conn,
        index=False,
        if_exists="replace",
        chunksize=1024,
        method="multi",
        dtype={
            "Изображение": JSON(),
            "Декларация": JSON(),
            "createdTime": DateTime(),
            "Created": DateTime(),
            "Updated": DateTime(),
            "timestamp": DateTime(),
            "Время создания": DateTime(),
            "Start date": DateTime(),
            "Время смены статуса": DateTime(),
            "Product-менеджер": JSON(),
            "Требования к печати приложение": JSON(),
            "Процентный состав": JSON(),
            "Deklaratsija from Notes from Deklaratsija i protokol ispytanij": JSON(),
            "Protokol ispytanij from from Deklaratsija i protokol ispytanij": JSON(),
            "Deklaratsija from Deklaratsija i protokol ispytanij": JSON(),
            "Protokol ispytanij from Deklaratsija i protokol ispytanij": JSON(),
            "Deklaratsija from Deklaratsija i protokol ispytanij 2": JSON(),
            "Protokol ispytanij from Deklaratsija i protokol ispytanij 2": JSON(),
            "Протокол испытаний": JSON(),
            "Как пришла идея": ARRAY(item_type=String),
            "Производство": ARRAY(item_type=String),
            "Тип волос/кожи/зубов": ARRAY(item_type=String),
            "Бренд": ARRAY(item_type=String),
            "Эффект 2 словами (рус)": ARRAY(item_type=String),
            "Время применения": ARRAY(item_type=String),
            "Действующие вещества INCI": ARRAY(item_type=String),
            "Тара (мн.выбор)": ARRAY(item_type=String),
            "Штрихкод": ARRAY(item_type=String),
            "Ссылки на конфликтные продукты": ARRAY(item_type=String),
            "Декларация и протокол испытаний": ARRAY(item_type=String),
            "ГОСТ (2)": ARRAY(item_type=String),
            "Nel'zja ispol'zovat' sovmestno (ssylki)": ARRAY(item_type=String),
            "Ispol'zovat' s ostorozhnost'ju (ssylki)": ARRAY(item_type=String),
            "Активные компонент": ARRAY(item_type=String),
            "Funktsii (from Dejstvujuschie veschestva)": ARRAY(item_type=String),
            "Konfliktnye Artikuly 1S (from Ssylki na konflikty)": ARRAY(
                item_type=String
            ),
            "Перечень конфликтов": ARRAY(item_type=String),
            "Perechen' (Nel'zja ispol'zovat' sovmestno)": ARRAY(item_type=String),
            "Artikuly (Nel'zja ispol'zovat' sovmestno)": ARRAY(item_type=String),
            "Perechen' (Ispol'zovat' s ostorozhnost'ju)": ARRAY(item_type=String),
        },
    )

    with Session(sql_conn) as session:
        session.execute("DELETE FROM sa.airtable_all_products")
        session.execute(
            "INSERT INTO sa.airtable_all_products SELECT * FROM sa.tmp_airtable_all_products"
        )
        session.execute("DROP TABLE sa.tmp_airtable_all_products")
        session.commit()


dag = DAG(
    "airtable_all_products",
    description="Выгрузка данных по всем продуктам из Airtable",
    schedule_interval="@daily",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    default_args={
        "on_failure_callback": on_failure_callback,
        "owner": "Brovko.NS",
    },
)
