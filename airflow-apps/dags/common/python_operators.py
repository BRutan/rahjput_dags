from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from copy import deepcopy
import dateutil.parser as dtparser
import logging
import os
import pandas as pd
import psycopg2
import re
import sys

def batch_elements(**context):
    """
    * Divide up items into 
    batches. To serve as a DAG step.
    Inputs:
    * items
    Returns:
    * 
    """
    pass

def read_file(filepath):
    """
    * Read content from file.
    """
    with open(filepath, 'r') as f:
        return f.read()
    
def list_str_to_list(list_str):
    """
    * Convert string containing list information
    (from xcom in particular) to list.
    """
    elemPattern = re.compile('(?P<elem>[^\[\],]+)')
    results = elemPattern.findall(list_str)
    output = [result.strip('"') for result in results]
    return output

def get_variable_values(**context):
    """
    * Return dicionary containing all
    variables mapped directly to the variable name.
    Inputs:
    * variables_list: List containing strings or tuples containing (variable_name (str), default_var (any)).
    Returns:
    * variable_values: Dictionary pointing {variable_name -> value}.
    """
    errs = []
    log = context.get('log', None)
    variables_list = context.get('variables_list', None)
    if log is None:
        errs.append('log is a required variable.')
    if variables_list is None:
        errs.append('variables_list is required.')
    elif not isinstance(variables_list, list):
        errs.append('variables_list must be a list.')
    elif not all([isinstance(variable, (str, tuple)) for variable in variables_list]):
        errs.append('variables_list must only contain strings or tuples.')
    if errs:
        raise ValueError('\n'.join(errs))
    # Gather all variables, note which are blank if requested:
    missing = []
    variable_values = {}
    for variable in variables_list:
        value = Variable.get(variable, default_var=None)
        if value is None:
            missing.append(variable)
        else:
            variable_values[variable] = value
    if missing:
        msg = f'The following variables were missing: {",".join(missing)}'
        log.warn(msg)
        raise ValueError(msg)
    context['ti'].xcom_push(key='variable_values', value=variable_values)
    
def get_tickers(**context):
    """ Get tickers to track from table.
    """
    log = context['log']
    log.info('Starting get_tickers().')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_connect = pg_hook.get_conn()
    cursor = pg_connect.cursor()
    variable_values = context['ti'].xcom_pull(task_ids='get_variables', key='variable_values')
    tickers_to_track_table = variable_values['tickers_to_track_table']
    log.info('Getting tickers needed to be tracked from %s.', tickers_to_track_table)
    tickers_to_track = cursor.execute(f'SELECT * FROM {tickers_to_track_table}')
    context['ti'].xcom_push(key='tickers_to_track', value=tickers_to_track)
    
def get_columns_to_write(**context):
    """ Get all columns to write from target table.
    """
    log = context['log']
    log.info('Starting get_columns_to_write().')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_connect = pg_hook.get_conn()
    cursor = pg_connect.cursor()
    table_name = context['table_name'].lower()
    if '.' in table_name:
        schema_name, table_name = table_name.split('.')
    else: 
        schema_name = context.get('schema_name', None)
    log.info(f'Getting columns need to pull from {table_name}.')
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'"
    if schema_name is not None:
        query += f" AND table_schema='{schema_name.lower()}'"
    log.info(query)
    cursor.execute(query)
    target_columns = cursor.fetchall()
    target_columns = [elem[0] for elem in target_columns]
    log.info('Using columns: %s', target_columns)
    context['ti'].xcom_push(key='columns_to_write', value=target_columns)
    
def get_and_validate_conf(**context):
    """ Get and validate conf variables passed to DAG.
    """ 
    log = context['log']
    conf = context['conf']
    required = context['required']
    optional = context.get('optional', {})
    missing = []
    invalid = {'name' : [], 'expected' : [], 'actual': []}
    for var in required:
        if not var in conf:
            missing.append(var)
        elif not isinstance(conf[var], required[var]):
            invalid['name'].append(var)
            invalid['expected'].append(str(required[var]))
            invalid['actual'].append(str(type(conf[var])))
    for var in optional:
        if var in conf and not isinstance(conf[var], required[var]):
            invalid['name'].append(var)
            invalid['expected'].append(str(required[var]))
            invalid['actual'].append(str(type(conf[var])))
    errs = []
    if missing:
        errs.append('The following required conf inputs missing:')
        errs.append(','.join(missing))
    if len(invalid['name']) > 0:
        invalid = pd.DataFrame(invalid)
        errs.append('The following conf variables had invalid types:')
        log.warn(errs[-1])
        log.warn(invalid)
        errs.append(','.join(invalid['name']))
    if errs:
        for err in errs:
            log.warn(err)
        log.warn('Invalid: ')
        log.warn(invalid)
        raise Exception('One or more conf errors occurred.')
    context['ti'].xcom_push(key='conf', value=conf)