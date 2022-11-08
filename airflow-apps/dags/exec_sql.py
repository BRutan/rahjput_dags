from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_and_validate_conf, get_dag_name, get_logger, get_variable_values
from datetime import datetime
from jinja2 import Template
import logging
import os
import pandas as pd
import sqlparse
import sys


def execute_sql(**context):
    """ Execute individual sql statement passed as argument.
    """
    sql = context['sql']
    log = context['log']
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    file_path = context.get('file_path', None)
    email = context.get('email', None)
    with pg_hook.get_connect() as pg_conn:
        cursor = pg_conn.cursor()
        
        
        
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="@once"
) as dag:
    
    log = get_logger(__file__)
    dag_run = DagRun(dag_id=dag.dag_id)
    conf = dag_run.conf
    
    
    op_kwargs = {'log' : log}
    start = EmptyOperator(task_id='start', dag=dag)
    
    get_and_validate_conf_task = PythonOperator(task_id='get_and_validate_conf',
                                        python_callable=get_and_validate_conf,
                                        op_kwargs={'log' : log, 'conf' : conf}
                                        provide_context=True,
                                        dag=dag)
    
    execute_sql_task = PythonOperator(task_id='execute_sql',
                                      python_callable=execute_sql,
                                      op_kwargs=op_kwargs,
                                      provide_context=True,
                                      dag=dag)
    
    
    start >> get_and_validate_conf_task >> execute_sql_task