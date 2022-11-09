###############################
# helpers.py
###############################
# Description:
# * 

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from copy import deepcopy
import logging
import os
import pandas as pd
import psycopg2
import re
import sys

def get_firstname_lastname(name, firstName=True):
    """
    * Extract firstname and lastname
    from string. 
    """
    name = name.split(' ')
    if name and firstName:
        return name[0]
    elif not firstName:
        if len(name) > 1:
            return name[1]
        else:
            return ''

def get_dag_name(filepath):
    """
    * Get the dag name to use with the
    DAG.
    Inputs:
    * dag_path: string pointing to dag file.
    """
    if not isinstance(filepath, str):
        raise ValueError('filepath must be a string.')
    dag_name = os.path.split(filepath)[1].replace('.py', '')
    return dag_name

def insert_data_pandas(data, kwargs):
    """
    * 
    """
    pass

def map_and_merge_data(data_with_mappers, standardize=False, inplace=False, log=None):
    """
    * Map data, merge into final dataset 
    from multiple sources.
    Inputs:
    * data_with_mappers: Dictionary mapping {'data_name' -> (data, mapper)}
    """
    
    pass

def map_data(data, mapper, standardize=False, inplace=False, log=None, null_if_missing=False):
    """
    * Map and convert data, usually
    before insertion into sql table.
    Inputs:
    * data: 
    """
    output_data = {}
    errs = []
    if isinstance(data, list):
        for dataset in data:
            result, errs = map_data(data=dataset,mapper=mapper,standardize=standardize,inplace=inplace,log=log,null_if_missing=False)
            output_data.update(result)
    elif isinstance(data, dict):
        for orig_key in mapper:
            try:
                if not null_if_missing and not orig_key in data:
                    continue
                if isinstance(mapper[orig_key], list):
                    # Map to multiple output columns:
                    for mapping in mapper[orig_key]:
                        #Test:
                        log.info(f'orig_key: {orig_key}')
                        log.info(f'mapping: {mapping}')
                        result, errs = map_data(data=data,mapper={orig_key : mapping},standardize=standardize,inplace=inplace,log=log,null_if_missing=False)
                        output_data.update(result)
                elif isinstance(mapper[orig_key], tuple) and (len(mapper[orig_key]) == 2) and hasattr(mapper[orig_key][1], '__call__'):
                    # Apply conversion specified in tuple:
                    mapped_key = mapper[orig_key][0]
                    converter = mapper[orig_key][1]
                    if standardize:
                        mapped_key = mapped_key.lower()
                    output_data[mapped_key] = converter(data[orig_key])
                elif isinstance(mapper[orig_key], str):
                    mapped_key = mapper[orig_key]
                    if standardize:
                        mapped_key = mapped_key.lower()
                    output_data[mapped_key] = data[orig_key]
            except Exception as ex:
                errs.append(f'{orig_key}:{str(ex)}')
    return output_data, errs
    
def get_columns_from_map(col_maps, standardize=False, log=None):
    """
    * Get target columns from the provided mappers.
    """
    target_columns = []
    if isinstance(col_maps, list):
        #Test:
        log.info(f'col_maps: type: {type(col_maps)} value: {col_maps}')
        for curr_map in col_maps:
            target_columns.extend(get_columns_from_map(curr_map, standardize=standardize, log=log))
    elif isinstance(col_maps, dict):
        for key in col_maps:
            if isinstance(col_maps[key], str):
                target_columns.append(col_maps[key])
            elif isinstance(col_maps[key], tuple):
                target_columns.append(col_maps[key][0])
            elif isinstance(col_maps[key], list):
                for mapping in col_maps[key]:
                    result = get_columns_from_map(col_maps[key], standardize=standardize, log=log)
                    target_columns.extend(result)
    if standardize and target_columns:
        for idx in range(len(target_columns)):
            target_columns[idx] = target_columns[idx].lower()
    return target_columns
    
def insert_in_batches(cursor, table_name, data, incl_columns=None, batch_size=1, log=None):
    """
    * Insert into table in batches.
    Inputs:
    * data: list of dictionaries containing data to insert.
    """
    columns = [key for key in data[0]] if (incl_columns is None) else incl_columns
    # Test:
    log.info(f'columns: {columns}')
    log.info(f'data: {data}')
    query_start = f"""
    INSERT INTO {table_name}
    ({",".join([col for col in columns])})
    VALUES"""
    query_str = []
    for start_idx in range(0, len(data), batch_size):
        end_idx = min(len(data), start_idx + batch_size)
        to_insert = data[start_idx : end_idx]
        for elem in to_insert:
            vals = [str(elem[column]) for column in columns]
            insert_line = f"({','.join(vals)})"
        query_str.append(insert_line)
    query_str.insert(0, query_start)
    query_str = ",\n".join(query_str)
    #Test:
    log.info('Query_str: ')
    log.info(query_str)
    cursor.execute(query_str)
    cursor.fetchall()
    
def insert_pandas(data, conn, table_name, schema, log, chunksize):
    if not isinstance(data, dict):
        log.info('data must be a dictionary.')
    data = pd.DataFrame(data)
    data.to_sql(name=table_name,con=conn,schema=schema,if_exists='fail',chunksize=chunksize)
        
##############
# Operators:
##############
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

def get_logger(filepath):
    """
    * Output log object.
    """
    return logging.getLogger(filepath)

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
    table_name = context['table_name']
    if '.' in table_name:
        schema_name, table_name = table_name.split('.')
    else: 
        schema_name = context.get('schema_name', None)
    log.info(f'Getting columns need to pull from {table_name}.')
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'"
    if schema_name is not None:
        query += f" AND table_schema='{schema_name}'"
    log.info(query)
    cursor.execute(query)
    target_columns = cursor.fetchall()
    log.info('target_columns:')
    log.info(target_columns)
    target_columns = [elem[0] for elem in target_columns]
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
    if invalid:
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