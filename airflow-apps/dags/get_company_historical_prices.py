
#from airflow.models import Variable
#from airflow.models.dag import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.operators.python_operator import PythonOperator
#from airflow.utils.session import provide_session
#from airflow.models.xcom import XCom
from common import *
from datetime import datetime, timedelta
import os
import re
import sys
import yfinance

dag_name = get_dag_name(__file__)
log = get_logger(__file__)


def get_prices(**kwargs):
    """
    * Get historical prices.
    """
    pass


