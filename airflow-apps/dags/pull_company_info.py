from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from common import get_columns_to_write, get_dag_name, get_logger, get_variable_values, get_tickers 
from datetime import datetime, timedelta
import os
import re
import sys
import yfinance

dag_name = get_dag_name(__file__)
log = get_logger(__file__)

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="@once"
) as dag:
    pass