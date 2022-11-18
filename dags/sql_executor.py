from datetime import datetime, timedelta
from functools import partial

from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.helpers import chain
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

        if type(config["query"]) != list:
            query = [config["query"]]
        else:
            query = config["query"]

        def execute_sql(query: str):
            """
            Процедура для выполнения SQl запроса. Открывает соединение с базой делает запрос, делает комит.
            Вход:
            query - SQL запрос который необходимо выполнить
            """
            try:
                conn_id = config["conn_id"]
            except KeyError:
                conn_id = "DWHPostgreSQL_dwa"
            sql_conn = get_postgres_engine(conn_id)
            with Session(sql_conn) as session:
                session.execute(query)
                session.commit()

        tasks = []

        for index, value in enumerate(query):
            tasks.append(
                PythonOperator(
                    task_id=f"execute_sql_{index}",
                    python_callable=partial(execute_sql, query=value),
                )
            )

        try:
            task_sensor = ExternalTaskSensor(
                task_id="task_sensor",
                external_dag_id=config["dependencies_dag_id"],
                execution_delta=timedelta(days=2),
                timeout=60,
                allowed_states=["success"],
                mode="reschedule",
                poke_interval=10,
                check_existence=True,
            )
            chain(task_sensor, *tasks)
        except KeyError:
            chain(*tasks)

    dynamic_generated_dag()
