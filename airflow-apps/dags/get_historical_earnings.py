from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from common import get_variable_values, connect_postgres
import logging
import yahoo_fin.stock_info as si