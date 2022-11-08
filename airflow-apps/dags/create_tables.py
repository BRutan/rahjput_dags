from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from jinja2 import Template
import os
import re
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

##############
# Steps:
##############
def get_tickers(**context) -> None:
    """Get tickers to create tables.
    """
    tickers_path = os.path.join(os.environ['PYTHONPATH'], 'dags', 'tickers.txt')
    log = context['log']
    if not os.path.exists(tickers_path):
        err_msg = f'{tickers_path} does not exist.'
        log.exception(err_msg)
        raise Exception(err_msg)
    with open(tickers_path, 'r') as f:
        tickers = f.read().split(',')
        tickers = [ticker.strip() for ticker in tickers if ticker.strip()]
        context['ti'].xcom_push(key='tickers', value=tickers)
        
def generate_schemas(**context):
    """ Create all schemas.
    """
    log = context['log']
    log.info('Starting generate_schemas().')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    if not os.path.exists('postgres/create_schemas.sql'):
        raise Exception('postgres/create_schemas.sql file is missing.')
    failed = []
    postgres_folder = os.path.join(os.environ['PYTHONPATH'], 'postgres')
    if not os.path.exists(postgres_folder):
        raise Exception(f'{postgres_folder} does not exist.')
    create_schemas_path = os.path.join(postgres_folder, 'create_schemas.sql')
    with open(create_schemas_path, 'r') as f:
        create_schema_stmts = f.read().split('\n')
        for stmt in create_schema_stmts:
            try:
                cursor.execute(stmt)
                cursor.fetchall()
            except Exception as ex:
                failed.append(f'"{stmt}", reason: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))
        
def generate_tables_from_templates(**context) -> None:
    """Generate tables from templates.
    """
    template_patt = re.compile(r'CREATE TABLE IF NOT EXISTS ([^\.]\.[^\s]) AS')
    tickers = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    postgres_folder = os.path.join(os.environ['PYTHONPATH'], 'postgres')
    if not os.path.exists(postgres_folder):
        raise Exception(f'{postgres_folder} does not exist.')
    template_paths = os.listdir(postgres_folder)
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
                    cursor.execute(rendered)
                    cursor.fetchall()
                    cursor.commit()
                except Exception as ex:
                    failed.append(f'{path}: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))     

def generate_tables(**context):
    """ Generate all non template tables.
    """
    log = context['log']
    log.info('Starting generate_tables.')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    postgres_folder = os.path.join(os.environ['PYTHONPATH'], 'postgres')
    log.info(f'Getting tables to generate from {postgres_folder}.')
    if not os.path.exists(postgres_folder):
        msg = 'postgres folder is missing.'
        log.exception(msg)
        raise Exception(msg)
    files = os.listdir(postgres_folder)
    files = [file for file in files if not file.endswith('_template.sql') and not file == 'create_schemas.sql']
    failed = []
    for file in files:
        with open(file, 'r') as f:
            try:
                cursor.execute(f.read())
                cursor.fetchall()
                cursor.commit()
                Variable.set(file[0:file.find('.sql')], file[0:file.find('.sql')])
            except Exception as ex:
                failed.append(f'{file} failed. Reason: {str(ex)}')
    if failed:
        for failure in failed:
            log.exception(failure)
        raise Exception('\n'.join(failed))
    
def insert_tickers_to_track(**context):
    """ Insert all tickers we want to track.
    """
    log = context['log']
    target_table = context['target_table']
    log.info('Starting generate_tables.')
    log.info(f'Inserting data into {target_table}.')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    tickers = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers')
    for num, ticker in enumerate(tickers):
        try:
            cursor.execute(f"INSERT INTO {target_table} (company_id, ticker) VALUES ({num + 1},'{ticker}')")
            cursor.fetchall()
            cursor.commit()
        except Exception as ex:
            log.exception(ex)
            raise ex
            

##############
# Dag:
##############
with DAG(
    dag_id='create_tables',
    start_date=datetime.now(),
    catchup=False,
    schedule='@once'
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    start = EmptyOperator(task_id = 'start', dag=dag)
    
    get_tickers_task = PythonOperator(task_id='get_tickers', 
                                      python_callable=get_tickers, 
                                      provide_context=True, 
                                      op_kwargs={'log':log},
                                      dag=dag)

    generate_table_templates_task = PythonOperator(task_id='generate_tables_from_templates',
                                                   python_callable=generate_tables_from_templates,
                                                   provide_context=True,
                                                   op_kwargs={'log':log, 'conn_id':'postgres_default'},
                                                   dag=dag)
    
    generate_tables_task = PythonOperator(task_id='generate_tables',
                                          python_callable=generate_tables,
                                          provide_context=True,
                                          op_kwargs={'log':log, 'conn_id':'postgres_default'},
                                          dag=dag)
    
    insert_tickers_to_track_task = PythonOperator(task_id='insert_tickers_to_track',
                                                  python_callable=insert_tickers_to_track,
                                                  provide_context=True,
                                                  op_kwargs={'log':log, 'conn_id':'postgres_default', 'target_table':'tickers_to_track'},
                                                  dag=dag)
    

    start >> get_tickers_task >> [generate_table_templates_task, generate_tables_task] >> insert_tickers_to_track_task
    

    
    