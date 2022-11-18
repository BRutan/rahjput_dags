from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_email_subject, get_date_filter_where_clause, get_filename, is_datetime, get_dag_name
from datetime import datetime, timedelta
from dateutil.parser import parse as dtparse
import json
import logging
import os
import pandas as pd

##################
# Operators:
##################
def check_conf(**context):
    """
    * Check parameters
    """
    log = context["log"]
    log.info("Starting check_conf().")
    errs = []
    if not "required" in context:
        errs.append("required missing from context.")
    for required in context["required"]:
        if not required in context:
            errs.append(f"Param {required} is missing.")
        elif len(context["required"][required]) < 2 and not isinstance(context[required], context["required"][required][0]):
            errs.append(f"Param {required} must be of type {context['required'][required][0]}")
        elif len(context["required"][required]) == 2 and not context["required"][required][0](context[required]):
            errs.append(f"Param {required} does not meet condition.")
    if "optional" in context:
        for optional in context["optional"]:
            if len(context["optional"][optional]) < 2 and not isinstance(context[optional], context["optional"][optional][0]):
                errs.append(f"Param {optional} must be of type {context['optional'][optional][0]}")
            elif len(context["optional"][optional]) == 2 and not context["optional"][optional][0](context[optional]):
                errs.append(f"Param {optional} does not meet condition.")
    if not errs and "exclusive" in context:
        for num, elem in enumerate(context["exclusive"]):
            if not elem(context):
                errs.append(f"context params failed exclusion param {num}.")
    if errs:
        raise ValueError("\n".join(errs))
    log.info("Ending check_conf().")
    
def pull_option_chains(**context):
    """
    * Pull option chains from sql instance and
    output to file.
    """
    log = context["log"]
    log.info("Starting pull_option_chains().")
    tickers = context["tickers"]
    tickers = set([ticker.lower() for ticker in tickers.split(",")])
    log.info("Setting date filter.")
    where_clause = get_date_filter_where_clause(context)
    where_clause = " ".join(where_clause)
    log.info(f"Date filter: {where_clause}")
    option_chain_table_outdir = Variable.get("option_chain_table_outdir")
    if not os.path.exists(option_chain_table_outdir):
        log.info(f"Creating option_chain_table_outdir since not exists at {option_chain_table_outdir}.")
        os.mkdir(option_chain_table_outdir)
    # Pull all columns for all passed tickers and output to file:
    log.info("Starting data pull.")
    filepaths = []
    option_chain_tables = Variable.get("option_chain_tables", {})
    pg_hook = PostgresHook(conn_id=context["conn_id"])
    with pg_hook.get_conn() as conn:
        for ticker in tickers:
            log.info(f"Getting results for ticker {ticker}.")
            option_chain_table = option_chain_tables[ticker]
            query = f"SELECT * FROM {option_chain_table}"
            query += where_clause
            data = pd.read_sql(sql=query, con=conn)
            log.info(f"Retrieved {len(data)} results.")
            # Write file:
            outpath = get_filename(ticker, 'option_chains', data, '.csv', context)
            log.info(f"Writing data to {outpath}.")
            filepath = os.path.join(option_chain_table_outdir, outpath)
            data.to_csv(outpath)
            filepaths.append(filepath)
    log.info("Ending pull_option_chains().")
    context["ti"].xcom_push(key="filepaths", value=filepaths)
    
def cleanup_files(**context):
    """
    * Remove output files.
    """
    log = context["log"]
    filepaths = context["filepaths"]
    log.info("Starting cleanup_files().")
    log.info(f"Deleting {len(filepaths)} after sending in email.")
    for filepath in filepaths:
        log.info(f"filepath: {filepath}")
        os.rmdir(filepath)
    log.info("Ending cleanup_files().")
    
###################
# Dag :


with DAG(
    dag_id=get_dag_name(__file__),
    start_date=datetime.now(),
    catchup=False,
    schedule="@once",
    params= {"tickers" : Param(type="string", default="", description="Tickers to pull. Must have table present."),
             "pull_date" : Param(type="string", default="", description="Day to pull, corresponding to upload_timestamp."),
             "start_date" : Param(type="string", default="", description="Start date to pull option chains."),
             "end_date" : Param(type="string", default="", description="End date to pull option chains.")}
) as dag:
    
    op_kwargs = dag.params
    
    if op_kwargs["tickers"] and any([op_kwargs["pull_date"], op_kwargs["start_date"], op_kwargs["end_date"]]):
        
        log = logging.getLogger()
        log.setLevel(logging.INFO)
        
        op_kwargs["log"] = log
        op_kwargs["conn_id"] = "postgres_default"
        
        start = EmptyOperator(task_id="start", 
                            dag=dag)
        
        
        required = {"tickers" : (str, lambda tickers : all([ticker in Variable.get("option_chains_tables", {}) for ticker in tickers.split(",")]))}
        optional = {"pull_date" : (str, is_datetime), "start_date" : (str, is_datetime), "end_date" : (str, is_datetime)}
        exclusive = [lambda x : not all([not x['pull_date'], not x['start_date'], not x['end_date']])]
        op_kwargs["required"] = required
        op_kwargs["optional"] = optional
        op_kwargs["exclusive"] = exclusive
        
        check_conf_task = PythonOperator(task_id="check_conf",
                                        op_kwargs=op_kwargs,
                                        python_callable=check_conf,
                                        dag=dag)
        
        
        pull_option_chains_task = PythonOperator(task_id="pull_option_chains",
                                        op_kwargs=op_kwargs,
                                        python_callable=pull_option_chains,
                                        dag=dag)
        
        subject = get_email_subject(op_kwargs["tickers"], "option chains", op_kwargs)
        filepaths = json.loads("{{ ti.filepaths }}")
        email_option_chain_task = EmailOperator(task_id="email_option_chains",
                                                to=Variable.get("key_persons_email").split(","),
                                                subject=subject,
                                                files=filepaths,
                                                dag=dag)
        
        cleanup_files_task = PythonOperator(task_id="cleanup_files",
                                            op_kwargs={"log" : log,
                                                    "filepaths" : filepaths},
                                            python_callable=cleanup_files,
                                            dag=dag)
        
        start >> check_conf_task >> pull_option_chains_task >> email_option_chain_task >> cleanup_files_task