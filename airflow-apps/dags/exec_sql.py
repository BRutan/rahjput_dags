from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_and_validate_conf, get_dag_name, get_logger, get_variable_values
from datetime import datetime
import logging
import os
import pandas as pd
import re
import sqlparse
import sys

DDL_PATTS = ["CREATE","DROP"]
DML_PATTS = ["SELECT"]

def execute_sql(**context):
    """ Execute individual sql statement passed as argument.
    """
    log = context['log']
    log.info('Starting execute_sql.')
    is_ddl = re.compile("|".join(DDL_PATTS))
    is_dml = re.compile("|".join(DML_PATTS))
    conf = context.xcom_pull(task_ids='get_and_validate_conf', key='conf')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    sql = conf['sql']
    file_path = conf.get('file_path', None)
    email = conf.get('email', None)
    # Check the sql:
    statements = sqlparse.split(sql)
    if len(statements) == 0:
        log.info('No sql statements provided.')
    with pg_hook.get_connect() as pg_conn:
        cursor = pg_conn.cursor()
        for statement in statements:    
            try:
                parsed = sqlparse.parse(statement)
                if not parsed:
                    continue
                log.info(sql)
                cursor.execute(statement)
                if is_dml.search(statement):
                    result = cursor.fetchall()
                    if file_path or email:
                        if file_path:
                            f = open(file_path, 'w+')
                            f.write(result)
                            f.close()
                        if email:
                            pass
                    else:
                        log.info(f'Result of {statement}:')
                        log.info(result)
            except Exception as ex:
                log.warn(f'Failed. Reason: {str(ex)}')
        
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
    required = {'sql' : (str)}
    optional = {'file_path' : (str), 'email' : (str)}
    
    get_and_validate_conf_task = PythonOperator(task_id='get_and_validate_conf',
                                                python_callable=get_and_validate_conf,
                                                op_kwargs={'log' : log, 
                                                           'conf' : conf,
                                                           'required':required,
                                                           'optional':optional},
                                                provide_context=True,
                                                dag=dag)
    
    execute_sql_task = PythonOperator(task_id='execute_sql',
                                      python_callable=execute_sql,
                                      op_kwargs={'log': log},
                                      provide_context=True,
                                      dag=dag)
    
    
    start >> get_and_validate_conf_task >> execute_sql_task