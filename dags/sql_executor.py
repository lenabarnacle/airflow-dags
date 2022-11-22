from datetime import datetime, timedelta
from functools import partial

from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.helpers import chain
from sqlalchemy.orm import Session
import networkx as nx

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine

configs = Variable.get(key="sql_executor", deserialize_json=True)


def execute_sql(query: str, conn_id: str = None):
    """
    Процедура для выполнения SQl запроса. Открывает соединение с базой делает запрос, делает комит.
    Вход:
    query - SQL запрос который необходимо выполнить
    conn_id - идентификатор соединения
    """
    if not conn_id:
        conn_id = "DWHPostgreSQL_dwa"
    sql_conn = get_postgres_engine(conn_id)
    with Session(sql_conn) as session:
        session.execute(query)
        session.commit()


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

    description = config.get("description")
    if not description:
        description = ""

    tags = config.get("tags")
    if not tags:
        tags = []

    @dag(
        dag_id=dag_id,
        description=description,
        schedule_interval=config["schedule_interval"],
        default_args=default_args,
        tags=tags,
        start_date=datetime(2022, 11, 1),
        catchup=False,
        max_active_runs=1,
    )
    def dynamic_generated_dag():
        """
        Функция для динамической генерации дагов на основе параметров храняхищся в переменных airflow

        Пример параметра (конфигурации):
        {
            "test_dag": {
                "conn_id": "DWHPostgreSQL_global_child",
                "tasks": [
                    {
                        "query": "select 1;",
                        "id": "main_task",
                        "description": "Возвращает 1"
                    },
                    {
                        "query": "select 2;",
                        "id": "dependency_task",
                        "dependency": "main_task",
                        "description": "Возвращает 2"
                    }
                ],
                "description": "",
                "schedule_interval": "*/5 * * * *",
                "owner": "Brovko.NS",
                "tags": [
                    "Administration DWH"
                ]
            }
        }

        Описания параметра (конфигурации):
        Ключ верхнего уровня - идентификатор DAG
        schedule_interval - частота выполнения DAG (cron expression)
        owner - владелец DAG
        tasks - список задач в даге, список JSON содержит атрибуты:
            query - запрос который необходимо выполнить
            id - идентификатор задачи
            description - описания задачи (НЕ ОБЯЗАТЕЛЬНЫЙ АТРИБУТ)
            dependency - зависимость задачи от другой задачи (передается её идентификатор), если список то от группы задач (НЕ ОБЯЗАТЕЛЬНЫЙ АТРИБУТ)
        conn_id - идентификатор соединения, которое будет использоваться для выполнения запросов (НЕ ОБЯЗАТЕЛЬНЫЙ АТРИБУТ)
        description - описания DAG (НЕ ОБЯЗАТЕЛЬНЫЙ АТРИБУТ)
        tags - массив с тегами DAG (НЕ ОБЯЗАТЕЛЬНЫЙ АТРИБУТ)

        Основные моменты в работе функции:
        1. Инициализируется направленный граф (networkx)
        2. Проверяем, что граф ацикличный
        3. Добавляем в граф ребра (зависимость, вершина)
        4. На основе графа в networkx строим граф зависимостей в Airflow
        В случае если в графе нет зависящих друг от друга задач,
        то он просто инициализируются, задачи добавленные в него будут паралельными

        Исключения (импорт дагов не произойдет):
        1. В случае если в рамках одного дага есть дубликаты идентификаторов задач
        2. В случае если полученый из конфигурации граф имеет циклы
        """

        if type(config["tasks"]) != list:
            tasks = [config["tasks"]]
        else:
            tasks = config["tasks"]

        if len(set(map(lambda x: x["id"], tasks))) != len(tasks):
            raise ImportError(
                f"В списке задач DAG({dag_id}) содержеться две или более задачи с одинаковым идентификатором"
            )

        tasks_list_dict = []
        di_graph = nx.DiGraph()

        for task in tasks:

            dependency = task.get("dependency")
            if dependency:
                if type(dependency) != list:
                    dependency = [dependency]

                for dep in dependency:
                    di_graph.add_edges_from([(dep, task["id"])])
            else:
                di_graph.add_node(task["id"])

            task_doc = task.get("description")
            if not task_doc:
                task_doc = ""

            tasks_list_dict.append(
                {
                    "id": task["id"],
                    "op": PythonOperator(
                        task_id=task["id"],
                        doc=task_doc,
                        python_callable=partial(
                            execute_sql,
                            query=task["query"],
                            conn_id=task.get("conn_id"),
                        ),
                    ),
                }
            )

        if not nx.is_directed_acyclic_graph(di_graph):
            raise ImportError(
                f"В списке задач DAG({dag_id}) содержатся циклические зависимости. Циклических зависимостей быть не должно"
            )

        di_graph = nx.transitive_reduction(di_graph)

        for edge in di_graph.edges():
            chain(
                next(filter(lambda x: x["id"] == edge[0], tasks_list_dict))["op"],
                next(filter(lambda x: x["id"] == edge[1], tasks_list_dict))["op"],
            )

    dynamic_generated_dag()
