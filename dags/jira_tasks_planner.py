from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

from callbacks import on_failure_callback
from dwh_resources import get_postgres_engine, get_jira_basic_auth

from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
import pandas as pd
import requests
import json


dag = DAG(
    "jira_tasks_planner",
    description="Очередность задач из Jira (Power BI)",
    schedule_interval="15 * * * *",
    start_date=datetime(2023, 1, 1),
    tags=["jira", "powerbi", "reports"],
    catchup=False,
    max_active_runs=1,
    default_args={
        "on_failure_callback": on_failure_callback,
        "owner": "Pindiurina.EP",
        "sla": timedelta(hours=2),
        "email": ["elena.p@carely.group"],
        "email_on_retry": False,
        "email_on_failure": True,
        "max_active_runs": 1,
    },
)


def get_jira_tasks_data() -> pd.DataFrame:

    url = 'https://carely-group.atlassian.net/rest/api/3/search'
    auth = HTTPBasicAuth(*get_jira_basic_auth("JiraBasicAuth"))
    params = {'jql': f'project=KIJP'}
    headers = {'Accept': 'application/json'}

    issues = pd.DataFrame({
        'issue_key': [],
        'issue_link': [],
        'direction': [],
        'priority': [],
        'status': [],
        'reporter': [],
        'assignee': [],
        'created': [],
        'duedate': [],
        'summary': [],
        'in_progress': []
    })

    start_at = 0
    max_results = 0
    total = 100000

    while start_at < total:

        params = {'jql': f'project=KIJP and labels="OKR_key_metrics" order by rank ASC',
                  'startAt': start_at,
                  'maxResults': 250}
        response = requests.request("GET", url, headers=headers, auth=auth, params=params)
        result = json.loads(response.text)

        total = result['total']
        max_results = result['maxResults']
        start_at = start_at + max_results

        for issue in result['issues']:
            try:
                in_progress = 0
                if issue['fields']['customfield_10020'] and [x['name'] for x
                                                             in issue['fields']['customfield_10020']
                                                             if x['state'] == 'active']:
                    in_progress = 1

                issue = pd.DataFrame({
                    'issue_key': [issue['key']],
                    'issue_link': ['https://carely-group.atlassian.net/browse/' + issue['key']],
                    'direction': [issue['fields']['customfield_10374']['value']],
                    'priority': [issue['fields']['priority']['name'].lower()],
                    'status': [issue['fields']['status']['name']],
                    'reporter': [issue['fields']['reporter']['displayName']],
                    'assignee': [issue['fields']['assignee']['displayName']],
                    'created': [issue['fields']['created']],
                    'duedate': [issue['fields']['duedate']],
                    'summary': [issue['fields']['summary']],
                    'in_progress': [in_progress]
                })
            except TypeError:
                continue

            issues = issues.append(issue, ignore_index=True)

    issues = issues[(issues['status'] != 'Выполнена') & (issues['direction'] != 'Общие задачи')]
    issues = pd.concat([issues[issues['in_progress'] == 1], issues[issues['in_progress'] == 0]])
    issues = issues.reset_index(drop=True)

    issues['created'] = pd.to_datetime(issues['created'])
    issues['duedate'] = pd.to_datetime(issues['duedate'])

    return issues


def upload_table_to_dwh(issues: pd.DataFrame):

    engine = get_postgres_engine("DWHPostgreSQL")
    issues.to_sql(name='jira_tasks_planner',
                  con=engine,
                  if_exists='replace',
                  schema='sa',
                  chunksize=10024,
                  index=False,
                  method='multi')

    return


def __main__():

    issues = get_jira_tasks_data()
    upload_table_to_dwh(issues)


main = PythonOperator(task_id="branch_task", python_callable=__main__, dag=dag)

main
