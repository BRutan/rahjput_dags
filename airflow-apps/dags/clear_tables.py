from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_and_validate_conf, get_dag_name, get_logger, get_variable_values
from datetime import datetime
import logging
import pandas as pd
import re


def clear_tables(**context):
    """ Remove all records from target tables.
    """
    log = context['log']
    conf = context['ti'].xcom_pull(task_ids='', key='conf')
    target_table = re.compile(conf['table_name']) if conf['isregex'] else conf['table_name']
    target_schema = conf['schema']
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    with pg_hook.get_conn() as pg_conn:
        cursor = pg_conn.cursor()
        if isinstance(target_table, type(re.compile())):
            log.info(f'Deleting all records from tables matching pattern {target_table.pattern}.')
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE target_schema='{target_schema}'")
            table_names = cursor.fetchall()
            table_names = [elem[0] for elem in table_names if target_table.match(elem[0])]
            for table_name in table_names:
                cursor.execute(f'DELETE FROM {target_schema}.{table_name} WHERE 1 = 1')    
        else:
            log.info(f'Deleting all records from table {target_table} in schema {target_schema}.')
            cursor.execute(f'DELETE FROM {target_schema}.{target_table} WHERE 1 = 1')

###########
# Dag:
###########
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(1),
    schedule="@once"
) as dag:
    
    log = get_logger(__file__)
    conf = DagRun(dag_id=dag.dag_id).conf
    
    required = {'table_name': (str), 'schema' : (str)}
    optional = {'isregex' : (bool)}
    
    start = EmptyOperator(task_id='start', dag=dag)
    
    get_and_validate_conf_task = PythonOperator(task_id='get_and_validate_conf',
                                                python_callable=get_and_validate_conf,
                                                op_kwargs={'log':log, 
                                                            'conf':conf, 
                                                            'required' : required,
                                                            'optional':optional},
                                                provide_context=True)
    
    clear_tables_task = PythonOperator(task_id='clear_tables',
                                        python_callable=clear_tables,
                                        op_kwargs={'log' : log},
                                        provide_context=True)
    
    start >> get_and_validate_conf_task >> clear_tables_task