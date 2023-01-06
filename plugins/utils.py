import os
from os.path import abspath, dirname, join
from typing import List

from airflow.utils.email import send_email
import pandas as pd


def send_df_by_email(
    df: pd.DataFrame,
    file_name: str,
    to: List[str],
    subject: str,
    html_content: str,
    cc: List[str] = None,
    bcc: List[str] = None,
):
    """Отправить pd.DataFrame по почте

    Args:
        df (pd.DataFrame): DataFrame, который нужно отправить
        file_name (str): Именования названия файла в который будет преобразован DataFrame перед отправкой (*.xlsx)
        to (List[str]): Список получаталей
        subject (str): Тема письма
        html_content (str): HTML контент письма
        cc (List[str], optional): Адресаты в копии в письме. Defaults to None.
        bcc (List[str], optional): Адресаты в скрытой копии в письме. Defaults to None.
    """
    base_path = dirname(abspath(__file__))
    file_path = join(base_path, file_name)
    df.to_excel(file_path, index=False)
    send_email(
        to=to,
        subject=subject,
        html_content=html_content,
        cc=cc,
        bcc=bcc,
        files=[file_path],
    )
    os.remove(file_path)
