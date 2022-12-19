from datetime import datetime
import requests

from sqlalchemy.orm import Session

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import provide_session, NEW_SESSION

from callbacks import on_failure_callback

doc_md = """Обновления JWT для авторизации в solar stuff. 
В случае если в течении часа обновлений не было, то необходимо  авторизоваться через метод login. 
Передать данные в Connection в Airflow SolarStuffJWT и после чего проверить что этот DAG работает без ошибок.
Документацию к API можно почитать по ссылки: https://solar-staff.com/api/docs/#jwt
"""

dag = DAG(
    "SolarSuffRefreshJWT",
    description="Обновления JWT для авторизации в solar stuff",
    doc_md=doc_md,
    schedule_interval="*/15 * * * *",
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


@provide_session
def refresh_jwt(session: Session = NEW_SESSION):
    """
    Обновления JWT солар стафф по расписанию
    1. Получает новый JWT
    2. Обновляет его в базе данных Airflow

    Вход:
    session - для взаимодействия с базой данных airflow, подкидывается автоматически через декортор provide_session
    """

    connection = BaseHook.get_connection("SolarStuffJWT")

    json_auth = connection.extra_dejson

    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    data = {"refreshToken": json_auth["refreshToken"]}

    response = requests.post(
        "https://solar-staff.com/api/token/refresh", headers=headers, json=data
    )

    if response.status_code != 200:
        raise BaseException("Ошибка при обновлении JWT")

    connection.set_extra(response.text)
    session.add(connection)
    session.commit()


task = PythonOperator(
    task_id="SolarSuffRefreshJWT", python_callable=refresh_jwt, dag=dag
)

task
