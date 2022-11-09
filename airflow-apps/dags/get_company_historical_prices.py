
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.dates import days_ago
from common import get_columns_to_write, get_and_validate_conf, get_dag_name, get_variable_values, get_dag_name, get_logger
from datetime import datetime, timedelta
import os
import re
import sys
import yfinance


#############
# Tasks:
#############        
def get_and_insert_prices(**context):
    """
    * Get historical prices from yfinance and insert.
    """
    log = context['log']
    conf = context['ti'].xcom_pull(task_ids='get_and_validate_conf', key='conf')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    tickers = context['tickers']
    table_name = context['variables']
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    
        
dag_name = get_dag_name(__file__)
log = get_logger(__file__)

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="@once"
) as dag:
    
    dag_run = DagRun(dag_id = dag.dag_id)
    conf = dag_run.conf
    
    required = {''}
    optional = {''}
    start_task = EmptyOperator(task_id='start', dag=dag)
    
    get_and_validate_conf_task = PythonOperator(task_id='get_and_validate_conf',
                                        python_callable=get_and_validate_conf,
                                        provide_context=True,
                                        op_kwargs={'log': log, 
                                                   'conf': conf,
                                                   'required': required,
                                                   'optional': optional},
                                        dag=dag)
    
    get_and_insert_prices_task = PythonOperator(task_id='get_and_insert_prices',
                                        python_callable=get_and_insert_prices,
                                        provide_context=True,
                                        op_kwargs={'log':log, 'conn_id' : 'postgres_default'},
                                        dag=dag)
    
    
    start_task >> get_and_validate_conf_task  >> get_and_insert_prices_task
    


