from airflow.providers.telegram.operators.telegram import TelegramOperator


def on_failure_callback(context):
    """
    Отправка сообщения в телеграм чат с алертами при возникновении ошибки
    """
    send_message = TelegramOperator(
        task_id="fail_message_telegram",
        telegram_conn_id="TelegramAllerting",
        text=f"""
Task: {context.get('task_instance').task_id}  
DAG: {context.get('task_instance').dag_id} 
Execution Time: {context.get('execution_date').strftime("%Y-%m-%d %H:%M:%S")}
Exception: {context.get('exception')}
            """[:300],
    )
    return send_message.execute(context=context)
