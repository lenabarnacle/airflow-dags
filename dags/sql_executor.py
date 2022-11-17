from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy.orm import Session

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine

configs = Variable.get(key="sql_executor", deserialize_json=True)

for dag_id, config in configs.items():

    default_args = {
        "pool": "sql_executor",
        "owner": config["owner"],
        "on_failure_callback": on_failure_callback,
        "retries": 5,
        "retry_delay": timedelta(minutes=10),
        "email": ["nikita.br@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    }

    @dag(
        dag_id=dag_id,
        description=config["description"],
        schedule_interval=config["schedule_interval"],
        default_args=default_args,
        tags=config["tags"],
        start_date=datetime(2022, 11, 1),
        catchup=False,
        max_active_runs=1,
    )
    def dynamic_generated_dag():
        """
        Функция для динамической генерации дагов на основе параметров храняхищся в переменных airflow
        Создает единственный таск execute_sql с задаными параметрами
        """

        @task
        def execute_sql(query: str):
            """
            Процедура для выполнения SQl запроса. Открывает соединение с базой делает запрос, делает комит.
            Вход:
            query - SQL запрос который необходимо выполнить
            """
            sql_conn = get_postgres_engine("DWHPostgreSQL")
            with Session(sql_conn) as session:
                session.execute(query)
                session.commit()

        execute_sql(config["query"])

    dynamic_generated_dag()
