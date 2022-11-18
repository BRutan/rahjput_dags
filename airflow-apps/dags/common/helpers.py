###############################
# helpers.py
###############################
# Description:
# * 
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
import datetime
from dateutil.parser import parse as dtparser
import logging
import os
import pandas as pd
import re
from typing import Any, Dict, List, Union

##################
# Type checking:
##################
def is_datetime(elem):
    try:
        dtparser(elem)
        return True
    except:
        return False
    
def is_regex(elem):
    try:
        if isinstance(elem, re.compile('')):
            return True
        re.compile(elem)
        return True
    except:
        return False
    
##################
# Helpers:
##################
def cleanup_files(**context):
    """
    * Remove output files.
    """
    log = context["log"]
    filepaths = context["filepaths"]
    pattern = context.get("fileregex", None)
    if pattern and is_regex(pattern):
        pattern = re.compile(pattern)
    log.info("Starting cleanup_files().")
    log.info(f"Deleting {len(filepaths)} files.")
    for filepath in filepaths:
        if not pattern is None and not pattern.match(filepath):
            log.info(f"Skipping since filepath {filepath} matches pattern {pattern.pattern}.")
            continue
        log.info(f"filepath: {filepath}")
        os.remove(filepath)
    log.info("Ending cleanup_files().")
    
def check_params(**context):
    """
    * Check parameters
    """
    log = context["log"]
    log.info("Starting check_params().")
    log.info("Checking the following params:")
    errs = []
    if not "required" in context:
        errs.append("required missing from context.")
    else:
        log.info("required:")
        log.info(context["required"])
    if "optional" in context:
        log.info("optional:")
        log.info(context["optional"])
    log.info("context: ")
    for param in context:
        if param in context["required"] or param in context["optional"] or param in context["exclusive"]:
            log.info(f"{param} : {context[param]}")
    for required in context["required"]:
        if not required in context:
            errs.append(f"Param {required} is missing.")
        elif len(context["required"][required]) < 2 and not isinstance(context[required], context["required"][required][0]):
            errs.append(f"Param {required} must be of type {context['required'][required][0]}")
        elif len(context["required"][required]) == 2 and not context["required"][required][0](context[required]):
            errs.append(f"Param {required} does not meet condition.")
    if "optional" in context:
        for optional in context["optional"]:
            if not(optional in context and context[optional]):
                continue
            if len(context["optional"][optional]) < 2 and not isinstance(context[optional], context["optional"][optional][0]):
                errs.append(f"Param {optional} must be of type {context['optional'][optional][0]}")
            elif len(context["optional"][optional]) == 2 and not context["optional"][optional][0](context[optional]):
                errs.append(f"Param {optional} does not meet condition.")
    if not errs and "exclusive" in context:
        for num, elem in enumerate(context["exclusive"]):
            if not elem(context):
                errs.append(f"context params failed exclusion param {num}.")
    if errs:
        raise AirflowFailException("\n".join(errs))
    log.info("Ending check_params().")
    
def get_email_subject(ticker : Union[List[str], str], content : str, context : Dict[str, Any]):
    """
    * Generate subject email using templated scheme.
    Args:
    - ticker: Ticker or tickers corresponding to data in email.
    - content: Content email is related to. Ex: option chains
    - context: Dictionary containing one or more of pull_date, start_date, end_date.
    """
    date_params = ["start_date", "end_date", "pull_date"]
    for param in date_params:
        if param in context and not isinstance(context[param], (datetime.datetime, datetime.date)) and is_datetime(context[param]):
            context[param] = dtparser(context[param])
    subject = f"{','.join(ticker) if isinstance(ticker, list) else ticker} {content}"
    if context["pull_date"] and is_datetime(context["pull_date"]):
       subject += f" for date {context['pull_date'].strftime('%m/%d/%y')}"
    elif context["start_date"] and context["end_date"] and is_datetime(context["start_date"]) and is_datetime(context["end_date"]):
         subject += f" for dates between {context['start_date'].strftime('%m/%d/%y')} and {context['end_date'].strftime('%m/%d/%y')} inclusive"
    elif context["start_date"] and is_datetime(context["start_date"]):
        subject += f" for dates after {context['start_date'].strftime('%m/%d/%y')} "
    elif context["end_date"] and is_datetime(context["end_date"]):
        subject += f" for dates before {context['end_date'].strftime('%m/%d/%y')} "
    return subject
         
def get_filename(ticker : str, content : str, data : pd.DataFrame, ext : str, context : Dict[str, Any]):
    """
    * Apply standard filenaming schema, i.e. <ticker>_<content>_(start_<start_date>_end_<end_date>_pulldate_<pull_date>).<ext>}
    Args:
    - ticker: Ticker associated with data.
    - content: Content description associated with data. Ex: option_chains
    - data: Pulled data. Expecting 'upload_timestamp' as column corresponding to start, end or pull date.
    - ext: File extension to apply.
    - context: op_kwargs passed to task.
    """
    date_params = ["start_date", "end_date", "pull_date"]
    for param in date_params:
        if param in context and not isinstance(context[param], (datetime.datetime, datetime.date)) and is_datetime(context[param]):
            context[param] = dtparser(context[param])
    outpath = f"{ticker.lower()}_{content.lower()}"
    if context["start_date"]:
        sd = max(max(data['upload_timestamp']), context['start_date'])
        outpath += f"_start_{sd.strftime('%m_%d_%y')}"
    if context["end_date"]:
        ed = min(min(data['upload_timestamp']), context['end_date'])
        outpath += f"_end_{ed.strftime('%m_%d_%y')}"
    if context["pull_date"] and not context["start_date"] and not context["end_date"]:
        pd = context["pull_date"]
        outpath += f"_on_{pd.strftime('%m_%d_%y')}"
    outpath += f".{ext.strip('.')}"
    return outpath
    
def get_date_filter_where_clause(**context):
    """
    * Take in date parameters, apply common filtering 
    logic across option chain tables.
    """
    where_clause = []
    date_params = ["start_date", "end_date", "pull_date"]
    for param in date_params:
        if param in context and context[param]:
            context[param] = dtparser(context[param])
    if "start_date" in context and context["start_date"] and "end_date" in context and context["end_date"]:
        if context["start_date"] > context["end_date"]:
            cpy = context["start_date"]
            context["start_date"] = context["end_date"]
            context["end_date"] = cpy
    # Pull for date range:
    if "start_date" in context and context["start_date"]:
        where_clause.append(f"upload_timestamp >= '{context['start_date'].strftime('%m-%d-%y')}'")
    if "end_date" in context and context["end_date"]:
        where_clause.append("AND" if len(where_clause) > 0 else "")
        where_clause.append(f"upload_timestamp <= '{context['end_date'].strftime('%m-%d-%y')}'")
    # Pull for single date:
    if "pull_date" in context and context["pull_date"] and not context["start_date"] and not context["end_date"]:
        dt = (context['pull_date'].month, context['pull_date'].day, context['pull_date'].year)
        where_clause.append(f"MONTH(upload_timestamp) = {dt[0]} AND DAY(upload_timestamp) = {dt[1]} AND YEAR(upload_timestamp) = {dt[2]}")
    if where_clause:
        where_clause.insert(0, "WHERE")
    
    return " ".join(where_clause)

def get_logger(filepath):
    """
    * Output log object.
    """
    return logging.getLogger(filepath)

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