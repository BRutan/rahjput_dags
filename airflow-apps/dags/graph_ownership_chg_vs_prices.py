from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_dag_name, get_logger
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import re
import sys

dag_name = get_dag_name(__file__)
log = get_logger(__file__)

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="@once"
) as dag:
    pass