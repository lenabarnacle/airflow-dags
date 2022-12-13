from datetime import datetime, timedelta
import logging

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from botocore import client
from sqlalchemy.orm import Session

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine, get_s3_client

dag = DAG(
    "sales_financial_department",
    description="Загрузка данных по продажам от финансового департамента из S3",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 11, 1),
    tags=["s3", "loaders"],
    catchup=False,
    max_active_runs=1,
    default_args={
        "on_failure_callback": on_failure_callback,
        "owner": "Brovko.NS",
        "sla": timedelta(minutes=15),
        "email": ["nikita.br@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def find_file(client: client.BaseClient, bucket: str) -> list:
    """
    Ищим файлы удовлетворяющие названию для загрузки
    1. Название файла должно удовлетворять маски all_sales_*
    2. Файл не должен называться all_sales_templates.xlsx
    Вход:
    client - клиент для работы с s3
    bucket - название корзины в которой искать файлы
    Выход:
    Список файлов в корзине удовлетворяющим названию
    """
    objects = client.list_objects(Bucket=bucket)
    data = objects["Contents"]
    df_s3_list = pd.DataFrame(data)
    df_s3_list = df_s3_list[df_s3_list["Key"].str.contains("all_sales_*")]
    df_s3_list = df_s3_list[df_s3_list["Key"] != "all_sales_templates.xlsx"]

    return df_s3_list["Key"].to_list()


def validate_columns(df: pd.DataFrame, col_mapper: dict) -> bool:
    """
    Проверка наименования столбцов в файле на соответствие
    Вход:
    df - Данные из s3
    col_mapper - словарь с сопостовление названия колонок на русском и английском
    Выход:
    True - проверку прошёл, False - проверку не прошёл
    """
    columns_file = set(df.columns)
    columns_target = set(col_mapper.keys())

    diff1 = columns_file - columns_target
    diff2 = columns_target - columns_file

    if not (diff2 == set() and diff1 == set()):
        return False

    return True


def validate_sales_month(df: pd.DataFrame) -> bool:
    """
    Проверка наличия символа x в строках столбца "Месяц"
    Вход:
    df - Данные из s3
    Выход:
    True - проверку прошёл, False - проверку не прошёл
    """
    check_x = df[df["Месяц"].str.contains("x")].shape[0]
    if check_x != df.shape[0]:
        return False

    return True


def processing(df: pd.DataFrame, col_mapper) -> pd.DataFrame:
    """
    Обработка данных перед загрузкой
    1. Приведение наименований столбцов в соответствие
    2. Приведение типов
    3. Удаление пустых строк
    Вход:
    df - Данные из s3
    col_mapper - словарь с сопостовление названия колонок на русском и английском
    Выход:
    Обработанные готовые к загрузки данные
    """
    df.columns = [col_mapper[col] for col in df.columns]

    df["sales_month"] = df["sales_month"].str.replace("x ", "")

    cols = [
        "quantity",
        "costs",
        "reg_customer_discount",
        "unit_price",
        "sales_sum",
        "agent_commission",
        "logistic_commission",
        "storage_commission",
        "sales_sum_without_nds",
        "agent_commission_without_nds",
        "logistic_commission_without_nds",
        "storage_commission_without_nds",
        "costs_without_nds",
        "goods_accept_commission",
        "goods_accept_commission_without_nds",
    ]

    for col in cols:
        df[col] = [np.nan if isinstance(x, str) else x for x in df[col]]
        df[col] = df[col].astype(float)

    for col in ["sales_month", "ozon_id"]:
        df[col] = np.where(pd.isnull(df[col]), df[col], df[col].astype(str))

    df = df.astype(
        {
            "sales_date": "datetime64[ns]",
            "sales_start_date": "datetime64[ns]",
            "order_date": "datetime64[ns]",
            "operation_date": "datetime64[ns]",
        }
    )

    df = df.dropna(subset=["product_article"]).reset_index(drop=True)

    return df


def upload_data(df: pd.DataFrame):
    """
    Загрузка данных в DWH
    1. Загрузить данные во временную таблицу
    2. Открываем транзакцию
    3. Удаляем старые данные с sales_month которые есть в новых данных
    4. Вставляем новые данных
    5. Удаляем временную таблицу
    6. Закрываем транзакцию
    Вход:
    df - данные которые нужно загрузить
    """

    sql_conn = get_postgres_engine("DWHPostgreSQL")

    df.to_sql(
        name="tmp_sales_financial_department",
        schema="sa",
        con=sql_conn,
        index=False,
        if_exists="replace",
        chunksize=1024,
        method="multi",
    )

    columns_str = ",".join(map(lambda col: f'"{col}"', df.columns))

    with Session(sql_conn) as session:
        session.execute(
            f"DELETE FROM sa.sales_financial_department WHERE sales_month IN (SELECT DISTINCT sales_month FROM sa.tmp_sales_financial_department)"
        )
        session.execute(
            f"INSERT INTO sa.sales_financial_department ({columns_str}) SELECT {columns_str} FROM sa.tmp_sales_financial_department"
        )
        session.execute("DROP TABLE sa.tmp_sales_financial_department")
        session.commit()


def __main__():
    """
    1. Ищем в корзине BUCKET нужные файлы
    2. Скачиваем их
    3. Загружаем лист с данными в excel
    4. Проверяем на соответствие название столбцов
    5. Обработка данных
    6. Загрузка данных
    7. Удаляем загруженный файл из S3
    """

    client = get_s3_client("LoggingS3Connection")

    COL_MAPPER = {
        "Наименование": "product_name",
        "Артикул": "product_article",
        "Кол-во": "quantity",
        "Себестоимость": "costs",
        "Скидки пост пок": "reg_customer_discount",
        "Цена за единицу": "unit_price",
        "Сумма продажи": "sales_sum",
        "Комиссия агента": "agent_commission",
        "Комиссия за логистику": "logistic_commission",
        "Комиссия за хранение": "storage_commission",
        "Отчет Агента №": "data_source",
        "Период": "sales_period",
        "Месяц": "sales_month",
        "Тип": "operation_type",
        "Канал": "sales_channel",
        "Клиент": "client",
        "Бренд": "brand",
        "Категория": "product_category",
        "Группа": "product_group",
        "Дата": "sales_date",
        "ИП/ООО": "entity",
        "Дата старта продаж": "sales_start_date",
        "Без НДС Сумма продажи": "sales_sum_without_nds",
        "Без НДС Комиссия агента": "agent_commission_without_nds",
        "Без НДС Комиссия за логистику": "logistic_commission_without_nds",
        "Без НДС Комиссия за хранение": "storage_commission_without_nds",
        "Без НДС Себестоимость": "costs_without_nds",
        "ИП/ООО.1": "country",
        "Номер заказа": "order_number",
        "Номер отправления": "delivery_number",
        "Дата заказа": "order_date",
        "Дата операции": "operation_date",
        "OZON id": "ozon_id",
        "Комиссия за приемку": "goods_accept_commission",
        "Без НДС Комиссия за приемку": "goods_accept_commission_without_nds",
    }

    BUCKET = "carely-upload-data"
    FILE_PATH = "/opt/airflow/tmp_data_sales_financial_department.xlsx"

    files_name = find_file(client, BUCKET)
    if files_name:
        for file_name in files_name:

            with open(FILE_PATH, "wb") as file:
                client.download_fileobj(BUCKET, file_name, file)

            df = pd.read_excel(FILE_PATH, sheet_name="All sales data")
            columns_name_cheсk = validate_columns(df, COL_MAPPER)
            # sales_month_check = validate_sales_month(df)
            try:
                email_data = pd.read_excel(FILE_PATH, sheet_name="info")
                email = email_data["email"][0]
            except:
                email = None
                # if not sales_month_check:
                #     if email:
                #         send_email(
                #             to=[email],
                #             subject=f"Отчет о загрузки файла {file_name}",
                #             html_content="В столбце 'Месяц' не все значения содержат x перед 'Датой'",
                #             cc=None,
                #             bcc=None,
                #         )
                #         logging.warning(
                #             f"В файле {file_name} в столбце 'Месяц' не все значения содержат x перед 'Датой'"
                #         )
                #         client.delete_object(Bucket=BUCKET, Key=file_name)

                return
            if not columns_name_cheсk:
                # Сообщить пользователю что валидация наименования столбцов не пройдена по email, который он указал
                if email:
                    send_email(
                        to=[email],
                        subject=f"Отчет о загрузки файла {file_name}",
                        html_content="Наименование столбцов не соответствует заявленым",
                        cc=None,
                        bcc=None,
                    )
                    logging.warning(
                        f"В файле {file_name} наименование столбцов не соответствует заявленым"
                    )
                    client.delete_object(Bucket=BUCKET, Key=file_name)
                return

            df = processing(df, COL_MAPPER)

            upload_data(df)

            client.delete_object(Bucket=BUCKET, Key=file_name)

            if email:
                send_email(
                    to=[email],
                    subject=f"Отчет о загрузки файла {file_name}",
                    html_content="Файл успешно загружен!",
                    cc=None,
                    bcc=None,
                )


main = PythonOperator(task_id="branch_task", python_callable=__main__, dag=dag)

main
