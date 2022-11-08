from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from common.helpers import connect_postgres
from datetime import datetime, timedelta
import logging
from jinja2 import Template
import os
from pathlib import Path
import re
import sys

sys.path.append(str(Path('../../').resolve()))

##############
# Steps:
##############
def get_tickers(**context) -> None:
    """Get tickers to create tables.
    """
    log = context['log']
    if not os.path.exists('tickers.txt'):
        err_msg = 'tickers.txt does not exist.'
        log.exception(err_msg)
        raise Exception(err_msg)
    with open('tickers.txt', 'r') as f:
        tickers = f.read().split(',')
        tickers = [ticker.strip() for ticker in tickers if ticker.strip()]
        context['ti'].xcom_push(key='tickers', value=tickers)
        
def generate_schemas(**context):
    """ Create all schemas.
    """
    log = context['log']
    log.info('Starting generate_schemas().')
    pg_conn = context['ti'].xcom_pull(task_ids='connect_postgres', key='postgres_conn')
    if not os.path.exists('postgres/create_schemas.sql'):
        raise Exception('postgres/create_schemas.sql file is missing.')
    failed = []
    with open('postgres/create_schemas.sql', 'r') as f:
        create_schema_stmts = f.read().split('\n')
        for stmt in create_schema_stmts:
            try:
                pg_conn.execute(stmt)
                pg_conn.fetchall()
            except Exception as ex:
                failed.append(f'"{stmt}", reason: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))
        
def generate_tables_from_templates(**context) -> None:
    """Generate tables from templates.
    """
    template_patt = re.compile(r'CREATE TABLE IF NOT EXISTS ([^\.]\.[^\s]) AS')
    pg_conn = context['ti'].xcom_pull(task_ids='connect_postgres', key='postgres_conn')
    tickers = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers')
    template_paths = os.listdir('postgres')
    template_paths = [path for path in template_paths if path.endswith('_template.sql')]
    failed = []
    log.info('Generating tables from templates for tickers.')
    for path in template_paths:
        with open(path, 'r') as f:
            content = f.read()
            template_val = template_patt.search(content)[0]
            Variable.set(path[0:path.find('.sql')], template_val)
            for ticker in tickers:
                try:
                    template = Template(content)
                    rendered = template.render(ticker=ticker)
                    pg_conn.execute(rendered)
                    pg_conn.fetchall()
                    pg_conn.commit()
                except Exception as ex:
                    failed.append(f'{path}: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))     

def generate_tables(**context):
    """ Generate all non template tables.
    """
    log = context['log']
    log.info('Starting generate_tables.')
    log.info('Connecting to rahjput_postgres.')
    pg_conn = context['ti'].xcom_pull(task_ids='connect_postgres', key='postgres_conn')
    log.info('Getting tables to generate from postgres folder.')
    if not os.path.exists('postgres'):
        msg = 'postgres folder is missing.'
        log.exception(msg)
        raise Exception(msg)
    files = os.listdir('postgres')
    files = [file for file in files if not file.endswith('_template.sql') and not file == 'create_schemas.sql']
    failed = []
    for file in files:
        with open(file, 'r') as f:
            try:
                pg_conn.execute(f.read())
                pg_conn.fetchall()
                pg_conn.commit()
                Variable.set(file[0:file.find('.sql')], file[0:file.find('.sql')])
            except Exception as ex:
                failed.append(f'{file} failed. Reason: {str(ex)}')
    if failed:
        for failure in failed:
            log.exception(failure)
        raise Exception('\n'.join(failed))
            

##############
# Dag:
##############
with DAG(
    dag_id='create_tables',
    start_date=days_ago(-1),
    catchup=False,
    schedule=None
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    start = EmptyOperator(task_id = 'start')
    
    connect_postgres_task = PythonOperator(task_id='connect_postgres',
                                           python_callable=connect_postgres,
                                           provide_context=True,
                                           op_kwargs={'log':log, 'conn_id': 'postgres_default'})
    
    get_tickers_task = PythonOperator(task_id='get_tickers', 
                                      python_callable=get_tickers, 
                                      provide_context=True, 
                                      op_kwargs={'log':log})

    generate_table_templates_task = PythonOperator(task_id='generate_tables_from_templates',
                                                   python_callable=generate_tables_from_templates,
                                                   provide_context=True,
                                                   op_kwargs={'log':log})
    
    generate_tables_task = PythonOperator(task_id='generate_tables',
                                          python_callable=generate_tables,
                                          op_kwargs={'log':log})
    

    start >> connect_postgres_task >> get_tickers_task >> [generate_table_templates_task, generate_tables_task]
    

    
    