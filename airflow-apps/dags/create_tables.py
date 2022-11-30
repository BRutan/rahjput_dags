from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from jinja2 import Template
import json
import os
import re
import sys

##############
# Steps:
##############
def get_tickers(**context) -> None:
    """Get tickers to create tables.
    """
    log = context['log']
    log.info("Getting all tickers from tickers_to_track variable.")
    tickers = Variable.get("tickers_to_track")
    tickers = tickers.split(",")
    tickers = [ticker.strip("'\"") for ticker in tickers if ticker.strip("'\"")]
    context['ti'].xcom_push(key='tickers', value=tickers)
        
def generate_schemas(**context):
    """ Create all schemas.
    """
    log = context['log']
    log.info('Starting generate_schemas().')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    postgres_folder = os.environ['POSTGRES_DEFS']
    if not os.path.exists(postgres_folder):
        raise Exception(f'{postgres_folder} does not exist.')
    create_schemas_path = os.path.join(postgres_folder, 'create_schemas.sql')
    if not os.path.exists(create_schemas_path):
        raise Exception(f'{create_schemas_path} file is missing.')
    failed = []
    with pg_hook.get_conn() as pg_conn:
        cursor = pg_conn.cursor()
        with open(create_schemas_path, 'r') as f:
            create_schema_stmts = f.read().split('\n')
            for stmt in create_schema_stmts:
                try:
                    log.info(stmt)
                    cursor.execute(stmt)
                except Exception as ex:
                    failed.append(f'"{stmt}", reason: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))
        
def generate_tables_from_templates(**context) -> None:
    """Generate tables from templates.
    """
    template_patt = re.compile(r'CREATE TABLE IF NOT EXISTS ([^\.]+\.[^\s]+)')
    tickers = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    postgres_folder = os.environ['POSTGRES_DEFS']
    if not os.path.exists(postgres_folder):
        raise Exception(f'{postgres_folder} does not exist.')
    template_files = os.listdir(postgres_folder)
    template_files = [path for path in template_files if path.endswith('_template.sql')]
    failed = []
    option_chains_tables = Variable.get('option_chains_tables', {}, deserialize_json=True)
    company_earnings_tables = Variable.get('company_earnings_tables', {}, deserialize_json=True)
    option_chains_tables_exists = len(option_chains_tables) != 0
    company_earnings_tables_exists = len(company_earnings_tables) != 0
    log.info('Generating tables from templates for tickers.')
    with pg_hook.get_conn() as pg_conn:
        cursor = pg_conn.cursor()
        for file in template_files:
            full_path = os.path.join(postgres_folder, file)
            with open(full_path, 'r') as f:
                content = f.read()
                for ticker in tickers:
                    try:
                        template = Template(content)
                        rendered = template.render(ticker=ticker)
                        log.info(rendered)
                        cursor.execute(rendered)
                        if file.startswith('option_chain'):
                            table_name = template_patt.search(rendered)[1]
                            option_chains_tables[ticker] = table_name.lower()
                        elif file.startswith('company_earnings'):
                            table_name = template_patt.search(rendered)[1]
                            company_earnings_tables[ticker] = table_name.lower()
                    except Exception as ex:
                        failed.append(f'{full_path}: {str(ex)}')
    if failed:
        raise Exception('\n'.join(failed))
    if option_chains_tables_exists:
        Variable.update(key='option_chains_tables', value=json.dumps(option_chains_tables))
    else:
        Variable.set(key='option_chains_tables', value=json.dumps(option_chains_tables))
    if company_earnings_tables_exists:
        Variable.update(key='company_earnings_tables', value=json.dumps(company_earnings_tables))
    else:
        Variable.set(key='company_earnings_tables', value=json.dumps(company_earnings_tables))

def generate_tables(**context):
    """ Generate all non template tables.
    """
    log = context['log']
    log.info('Starting generate_tables.')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    postgres_folder = os.environ['POSTGRES_DEFS']
    log.info(f'Getting tables to generate from {postgres_folder}.')
    if not os.path.exists(postgres_folder):
        msg = 'postgres folder is missing.'
        log.exception(msg)
        raise Exception(msg)
    table_patt = re.compile('\d+_([^\.]+)\.sql')
    template_patt = re.compile(r'CREATE TABLE IF NOT EXISTS ([^\.]+\.[^\s]+)')
    files = os.listdir(postgres_folder)
    files = [file for file in files if table_patt.match(file)]
    files.sort()
    failed = []
    table_names = Variable.get('table_names', {}, deserialize_json=True)
    var_exists = len(table_names) != 0
    with pg_hook.get_conn() as pg_conn:
        cursor = pg_conn.cursor()
        for file in files:
            full_path = os.path.join(postgres_folder, file)
            with open(full_path, 'r') as f:
                try:
                    stmt = f.read()
                    log.info(stmt)
                    cursor.execute(stmt)
                    table_key = table_patt.search(file)[1]
                    full_table_name = template_patt.search(stmt)[1]
                    table_names[table_key] = full_table_name.lower()
                except Exception as ex:
                    failed.append(f'{full_path} failed. Reason: {str(ex)}')
    if failed:
        for failure in failed:
            log.exception(failure)
        raise Exception('\n'.join(failed))
    if var_exists:
        Variable.update(key='table_names', value=json.dumps(table_names))
    else:
        Variable.set(key='table_names', value=json.dumps(table_names))

def insert_tickers_to_track(**context):
    """ Insert all tickers we want to track.
    """
    log = context['log']
    target_table = context['target_table']
    target_schema = context['target_schema']
    log.info('Starting generate_tables.')
    log.info(f'Inserting data into {target_table}.')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    tickers = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers')
    with pg_hook.get_conn() as pg_conn:
        cursor = pg_conn.cursor()
        args = ','.join(cursor.mogrify("(%s)", (ticker,)).decode('utf-8') for ticker in tickers)
        stmt = f"INSERT INTO {target_schema}.{target_table} (ticker) VALUES {args} ON CONFLICT DO NOTHING"
        log.info(stmt)
        cursor.execute(stmt, tuple(tickers))
        # Update the tickers_to_track with already present tickers if some were removed:
        cursor.execute(f"SELECT ticker FROM {target_schema}.{target_table}")
        tickers_to_track = [elem[0] for elem in cursor.fetchall()]
        tickers_to_track.sort()
        diff = set(tickers_to_track) - set(tickers)
        if diff:
            diff = list(diff)
            diff.sort()
            log.info("Adding back the following tickers to 'tickers_to_track' variable since present in database: %s", ",".join(diff))
        Variable.set(key="tickers_to_track", value=",".join(tickers_to_track))
    
            
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
    
    generate_schemas_task = PythonOperator(task_id='generate_schemas',
                                           python_callable=generate_schemas,
                                           provide_context=True,
                                           op_kwargs={'log':log, 
                                                      'conn_id':'postgres_default'},
                                           dag=dag)

    generate_table_templates_task = PythonOperator(task_id='generate_tables_from_templates',
                                                   python_callable=generate_tables_from_templates,
                                                   provide_context=True,
                                                   op_kwargs={'log':log, 
                                                              'conn_id':'postgres_default'},
                                                   dag=dag)
    
    generate_tables_task = PythonOperator(task_id='generate_tables',
                                          python_callable=generate_tables,
                                          provide_context=True,
                                          op_kwargs={'log':log, 
                                                     'conn_id':'postgres_default'},
                                          dag=dag)
    
    insert_tickers_to_track_task = PythonOperator(task_id='insert_tickers_to_track',
                                                  python_callable=insert_tickers_to_track,
                                                  provide_context=True,
                                                  op_kwargs={'log':log, 
                                                             'conn_id':'postgres_default', 
                                                             'target_table':'tickers_to_track',
                                                             'target_schema':'rahjput_data'},
                                                  dag=dag)
    

    start >> get_tickers_task >> generate_schemas_task >> [generate_table_templates_task, generate_tables_task] >> insert_tickers_to_track_task
    

    
    