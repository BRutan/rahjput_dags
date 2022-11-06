from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.helpers import batch_elements, get_dag_name, get_columns_from_map, get_firstname_lastname, get_logger, get_all_variables, insert_pandas, insert_in_batches, map_data 
from datetime import datetime, timedelta
from interfaces import *
import json
import os
from parsers import *
import pandas as pd
import psycopg2
import re
import sys
import traceback

def connect_postgres(**kwargs):
    """
    * Connect to postgres instance.
    """
    log = kwargs['log']
    log.info(f'Attempting to connect to postgres instance using conn_id {kwargs["conn_id"]}.')
    try:
        pg_hook = PostgresHook(postgres_conn_id=kwargs['conn_id'])
        postgres_conn = pg_hook.get_conn()
        return postgres_conn
    except Exception as ex:
        msg = f'Failed to connect to postgres instance using conn_id {kwargs["conn_id"]}. Reason: str({ex}).'
        log.error(msg)
        raise AirflowSkipException(msg)
    
    
with DAG(dag_name=get_dag_name(__file__), 
         schedule_interval='@once',
         start_date=datetime.now(),
         catchup = False,
         default_args=default_args,
         # Output jinja templates as native object instead of string:
         render_template_as_native_obj=True,
         tags=['staffing']) \
as dag:
    
    validate_variables_task = PythonOperator(task_id='validate_variables',
                                             python_callable=validate_variables,
                                             provide_context=True,
                                             op_kwargs=op_kwargs)
    
    get_and_validate_args_task = PythonOperator(task_id='get_and_validate_args',
                                                python_callable=get_and_validate_args,
                                                op_kwargs=op_kwargs)

    download_and_parse_resumes_task = PythonOperator(task_id='download_and_parse_resumes',
                                        python_callable=download_parse_and_load_resumes,
                                        provide_context=True, 
                                        op_kwargs=op_kwargs) 

    validate_variables_task >> get_and_validate_args_task >> download_and_parse_resumes_task