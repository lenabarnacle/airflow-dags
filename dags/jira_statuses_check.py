from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine, get_jira_basic_auth
from utils import send_df_by_email

from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
import pandas as pd
import requests
import json


dag = DAG(
    "jira_statuses_check",
    description="Проверяем, соответствуют ли статусы в таблице sa.tis_settings актуальным статусам в Jira",
    schedule_interval="15 6 * * *",
    start_date=datetime(2022, 12, 1),
    tags=["jira", "checks"],
    catchup=False,
    max_active_runs=1,
    default_args={
        "on_failure_callback": on_failure_callback,
        "owner": "Pindiurina.EP",
        "sla": timedelta(minutes=15),
        "email": ["elena.p@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def get_jira_statuses_from_dwh() -> pd.DataFrame:

    """
    Импортирует таблицу sa.tis_settings из dwh.
    Таблица sa.tis_settings содержит нормативные сроки нахождения эпиков в статусах.
    Важно, чтобы названия статусов в таблице sa.tis_settings совпадали с актуальными статусами в Jira

    Вход:
    --

    Выход:
    Таблица sa.tis_settings
    """

    engine = get_postgres_engine("DWHPostgreSQL")
    tis_settings_q = "select * from sa.tis_settings"
    tis_settings = pd.read_sql(tis_settings_q, engine)
    return tis_settings


def check_mismatches(tis_settings: pd.DataFrame) -> pd.DataFrame:

    """
    Проверяет, соответствуют ли названия статусов в таблице sa.tis_settings актуальным статусам в Jira.
    Сообщает о несоответствиях, если таковые имеются

    Вход:
    Таблица sa.tis_settings

    Выход:
    None
    """

    auth = HTTPBasicAuth(*get_jira_basic_auth("JiraBasicAuth"))
    headers = {"Accept": "application/json"}

    projects = {"product": ["RES", "NPD"], "bundle": ["NS", "SET"]}

    curr_statuses = pd.DataFrame({"status_name": [], "setting_type": []})

    for key, values in projects.items():
        for project in values:
            url = f"https://carely-group.atlassian.net/rest/api/2/project/{project}/statuses"
            response = requests.request("GET", url, headers=headers, auth=auth)
            result = json.loads(response.text)
            for s in result:
                if s["name"] == "Epic":
                    for status in s["statuses"]:
                        status_dict = {
                            "status_name": status["name"],
                            "setting_type": key,
                        }
                        curr_statuses = curr_statuses.append(
                            status_dict, ignore_index=True
                        )
    curr_statuses = curr_statuses[
        curr_statuses["status_name"] != "Отклоненные"
    ].reset_index(drop=True)
    mismatches = curr_statuses.merge(
        tis_settings, how="left", on=["status_name", "setting_type"]
    )
    mismatches = mismatches[mismatches["status_order"].isna()][
        ["status_name", "setting_type"]
    ]
    mismatches = mismatches[
        mismatches["status_name"] != "Уточнение условий"
    ].reset_index(drop=True)

    if len(mismatches) > 0:
        send_df_by_email(
            to=["elena.p@carely.group"],
            subject="Статусы в sa.tis_settings не соответствуют статусам в Jira",
            html_content=f"""Найдены несоответствия: {len(mismatches)}. Необходимо внести изменения в google-таблицу 'Сроки нахождения продуктов в статусах.'""",
            df=mismatches,
            file_name="Несоответствие.xlsx",
        )

    return


def __main__():

    tis_settings = get_jira_statuses_from_dwh()
    check_mismatches(tis_settings)


main = PythonOperator(task_id="branch_task", python_callable=__main__, dag=dag)

main
