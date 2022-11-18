from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import cleanup_files, get_email_subject, get_date_filter_where_clause, get_filename, is_datetime, get_dag_name
from copy import deepcopy
from datetime import datetime
import json
import logging
import os
import pandas as pd

##################
# Operators:
##################
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
    
def pull_option_chains(**context):
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
    option_chains_table_out_dir = Variable.get("option_chains_table_out_dir")
    if not os.path.exists(option_chains_table_out_dir):
        log.info(f"Creating option_chain_table_outdir since not exists at {option_chains_table_out_dir}.")
        os.mkdir(option_chains_table_out_dir)
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
            filepath = os.path.join(option_chains_table_out_dir, outpath)
            log.info(f"Writing data to {filepath}.")
            data.to_csv(filepath)
            filepaths.append(filepath)
    log.info("Ending pull_option_chains().")
    context["ti"].xcom_push(key="filepaths", value=filepaths)
    
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
    
    pull_option_chains_task = PythonOperator(task_id="pull_option_chains",
                                            op_kwargs=op_kwargs,
                                            python_callable=pull_option_chains)
    try:
        filepaths = json.loads("{{ ti.xcom_pull(task_ids='pull_option_chains', key='filepaths') }}")
    except:
        filepaths = ""
        
    subject = get_email_subject(op_kwargs["tickers"], "option chains", op_kwargs)
    email_option_chain_task = EmailOperator(task_id="email_option_chains",
                                            to=Variable.get("key_persons_email").split(","),
                                            subject=subject,
                                            html_content="See attached.",
                                            files=filepaths)
    
    cleanup_files_task = PythonOperator(task_id="cleanup_files",
                                        op_kwargs={"log" : log,
                                                   "filepaths" : filepaths},
                                        python_callable=cleanup_files)
        
    start >> check_params_task >> pull_option_chains_task >> email_option_chain_task >> cleanup_files_task