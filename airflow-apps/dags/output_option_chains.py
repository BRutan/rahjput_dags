from airflow.exceptions import AirflowFailException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import check_params, get_date_filter_where_clause, get_filename, is_datetime, get_dag_name
from copy import deepcopy
from datetime import datetime
import json
import logging
import os
import pandas as pd

##################
# Operators:
##################
def pull_and_output_option_chains(**context):
    """
    * Pull option chains from sql instance and
    output to file.
    """
    log = context["log"]
    log.info("Starting pull_option_chains().")
    tickers = context["tickers"]
    tickers = set([ticker.lower().strip() for ticker in tickers.split(",")])
    log.info("Setting date filter.")
    where_clause = get_date_filter_where_clause(**context)
    log.info(f"Date filter: {where_clause}")
    option_chains_table_outdir = Variable.get("option_chains_table_outdir")
    if not os.path.exists(option_chains_table_outdir):
        log.info(f"Creating option_chain_table_outdir since not exists at {option_chains_table_outdir}.")
        os.mkdir(option_chains_table_outdir)
    # Pull all columns for all passed tickers and output to file:
    log.info("Starting data pull.")
    filepaths = []
    option_chains_tables = Variable.get("option_chains_tables", {}, deserialize_json=True)
    pg_hook = PostgresHook(conn_id=context["conn_id"])
    with pg_hook.get_conn() as conn:
        for ticker in tickers:
            log.info(f"Getting results for ticker {ticker.upper()}.")
            option_chains_table = option_chains_tables[ticker.upper()]
            query = f"SELECT * FROM {option_chains_table} "
            query += where_clause
            data = pd.read_sql(sql=query, con=conn)
            log.info(f"Retrieved {len(data)} results.")
            # Write file:
            outpath = get_filename(ticker.lower(), 'option_chains', data, '.csv', context)
            filepath = os.path.join(option_chains_table_outdir, outpath)
            log.info(f"Writing data to {filepath}.")
            data.to_csv(filepath)
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
###################
with DAG(
    dag_id=get_dag_name(__file__),
    start_date=datetime.today(),
    catchup=False,
    schedule_interval=None,
    params = {"tickers" : Param(type="string", default="", description="Tickers to pull. Must have table present."),
             "pull_date" : Param(type="string", default="", description="Day to pull, corresponding to upload_timestamp."),
             "start_date" : Param(type="string", default="", description="Start date to pull option chains."),
             "end_date" : Param(type="string", default="", description="End date to pull option chains.")},
    render_template_as_native_obj=True
    ) as dag:
    
    #dag.trigger_arguments = dag.params
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    
    op_kwargs = {}
    op_kwargs["tickers"] = "{{ params.tickers }}"
    op_kwargs["pull_date"] = "{{ params.pull_date }}"
    op_kwargs["start_date"] = "{{ params.start_date }}"
    op_kwargs["end_date"] = "{{ params.end_date }}"
    op_kwargs["log"] = log
    op_kwargs["conn_id"] = "postgres_default"
    
    required = {"tickers" : (str, lambda tickers : len(tickers) > 0 and all([ticker.upper() in Variable.get("option_chains_tables", deserialize_json=True) for ticker in tickers.split(",")]))}
    optional = {"pull_date" : (str, is_datetime), "start_date" : (str, is_datetime), "end_date" : (str, is_datetime)}
    exclusive = [lambda x : not all([not x['pull_date'], not x['start_date'], not x['end_date']])]
    op_kwargs["required"] = required
    op_kwargs["optional"] = optional
    op_kwargs["exclusive"] = exclusive
    
    start = EmptyOperator(task_id="start")
    
    check_params_task = PythonOperator(task_id="check_params",
                                       op_kwargs=op_kwargs,
                                       python_callable=check_params)
    
    pull_and_output_option_chains_task = PythonOperator(task_id="pull_and_output_option_chains",
                                                        op_kwargs=op_kwargs,
                                                        python_callable=pull_and_output_option_chains)
        
    start >> check_params_task >> pull_and_output_option_chains_task