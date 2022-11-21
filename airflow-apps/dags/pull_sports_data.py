from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import check_params, get_dag_name, is_datetime, is_float
from datetime import datetime, timedelta
from dateutil.parser import parse as dtparse
from mpl_toolkits import mplot3d
from datetime import datetime
from itertools import chain
import logging
from matplotlib import cm
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

###################
# Operators:
###################
def pull_sports_data(**context):
    """
    
    """
    pass

###################
# Dag:
###################
with DAG(
    dag_id=get_dag_name(__file__),
    start_date=datetime.today(),
    catchup=False,
    schedule_interval=None,
    render_template_as_native_obj=True
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    