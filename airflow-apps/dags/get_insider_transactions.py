
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from common import connect_postgres, get_columns_to_write, get_dag_name, get_logger, get_variable_values, get_tickers 
from datetime import datetime, timedelta
import logging
import requests
import os

dag_name = get_dag_name(__file__)
log = get_logger(__file__)

def get_and_insert_insider_transactions(**context):
    postgres_conn = context['ti'].xcom_pull(task_ids='connect_postgres', key='postgres_conn')
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
                                      op_kwargs={'log':log},
                                      dag=dag)
    
    connect_postgres_task = PythonOperator(task_id='connect_postgres',
                                        python_callable=connect_postgres,
                                        provide_context=True,
                                        op_kwargs={'conn_id':'postgres_default','log':log},
                                        dag=dag)
    
    get_columns_task = PythonOperator(task_id='get_columns',
                                      python_callable=get_columns_to_write,
                                      provide_context=True,
                                      op_kwargs={'log':log, 'table_name':'insider_transactions', 'schema_name':'data'},
                                      dag=dag)
    
    get_and_insert_transactions_task = PythonOperator(task_id=f'get_and_insert_insider_transactions',
                                                        python_callable=get_and_insert_insider_transactions,
                                                        provide_context=True,
                                                        op_kwargs={'log': log, 
                                                                   'target_table' : 'insider_transactions', 
                                                                   'target_schema' : 'data'},
                                                        dag=dag)
        
    start >> get_variables_task >> get_tickers_task >> connect_postgres_task >> get_columns_task >> get_and_insert_transactions_task 


