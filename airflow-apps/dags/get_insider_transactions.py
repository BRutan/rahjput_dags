
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from common import get_columns_to_write, get_dag_name, get_logger, get_variable_values, get_tickers 
from datetime import datetime, timedelta
import logging
import requests
import os

dag_name = get_dag_name(__file__)
log = get_logger(__file__)

def get_and_insert_insider_transactions(**context):
    
    api_key = context['api_key']
    log = context['log']
    schedule_interval = context['schedule_interval']
    scheduler = context['scheduler']
    ticker = context['ticker']
    end_time = context['end_time']
    target_table = context['target_table']
    columns_to_write = context['ti'].xcom_pull(task_ids='get_columns')
    result = requests.get(f'https://finnhub.io/api/v1/stock/insider-transactions?symbol={ticker}&token={api_key}')
    
    
    
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="30 9 * * 1-5"
) as dag:
    
    log = get_logger(__file__)
    log.setLevel(logging.INFO)
    
    start = EmptyOperator(task_id='start')
    
    get_variables_task = PythonOperator(task_id='get_variables',
                                        python_callable=get_variable_values, 
                                        provide_context=True,
                                        op_kwargs={'variable_list' : ['tickers_to_track_table'], 
                                                   'log':log},
                                        dag=dag)
    
    get_tickers_task = PythonOperator(task_id='get_tickers',
                                      python_callable=get_tickers,
                                      provide_context=True,
                                      op_kwargs={'log':log,
                                                 'conn_id' : 'postgres_default'},
                                      dag=dag)
    
    get_columns_task = PythonOperator(task_id='get_columns',
                                      python_callable=get_columns_to_write,
                                      provide_context=True,
                                      op_kwargs={'log':log, 
                                                 'table_name':'insider_transactions', 
                                                 'schema_name':'data', 
                                                 'conn_id':'postgres_default'},
                                      dag=dag)
    
    get_and_insert_transactions_task = PythonOperator(task_id=f'get_and_insert_insider_transactions',
                                                        python_callable=get_and_insert_insider_transactions,
                                                        provide_context=True,
                                                        op_kwargs={'log': log, 
                                                                   'target_table' : 'insider_transactions', 
                                                                   'target_schema' : 'data',
                                                                   'conn_id' : 'postgres_default'},
                                                        dag=dag)
        
    start >> get_variables_task >> get_tickers_task >> get_columns_task >> get_and_insert_transactions_task 


