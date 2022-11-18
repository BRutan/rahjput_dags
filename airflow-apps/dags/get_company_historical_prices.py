
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.dates import days_ago
from common import check_params, get_columns_to_write, get_dag_name, is_datetime
from datetime import datetime, timedelta
from dateutil.parser import parser as dtparser
import logging
from jinja2 import Template
import json
import os
import pandas as pd
import re
import yfinance


#############
# Tasks:
#############
def generate_tables(**context):
    """
    * Generate tables to store stock prices with templates.
    """
    log = context["log"]
    tickers = [ticker.upper() for ticker in context["tickers"].split(",")]
    log.info("Starting generate_or_get_tables().")
    template_patt = re.compile(r'CREATE TABLE IF NOT EXISTS ([^\.]+\.[^\s]+)')
    table_names = {}
    # Ensure key folders and files exist:
    if not 'POSTGRES_DEFS' in os.environ:
        raise AirflowException("'POSTGRES_DEFS' environment variable is not configured.")
    if not os.path.exists(os.environ['POSTGRES_DEFS']):
        raise AirflowException(f"Postgres definitions folder does not exist at {os.environ['POSTGRES_DEFS']}.")
    template_file_path = os.path.join(os.environ['POSTGRES_DEFS'], "/historical_stock_prices_template.sql")
    if not os.path.exists(template_file_path):
        raise AirflowException(f"Template file path {template_file_path} is missing.")
    create_table_template = ""
    with open(template_file_path, "r") as f:
        create_table_template = Template(f.read())
    historical_stock_prices_tables = Variable.get("historical_stock_prices_tables", default={}, deserialize_json=True)
    var_exists = len(historical_stock_prices_tables) != 0
    pg_hook = PostgresHook(conn_id=context["conn_id"])
    # Generate all of the tables, update the historical_stock_prices_tables variable:
    log.info("Generating all historical stock price tables and updating associated variable.")
    table_names = {}
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        for ticker in tickers:
            if ticker in historical_stock_prices_tables:
                log.info(f"Table for ticker {ticker} already exists at {historical_stock_prices_tables[ticker]}.")
            # Make sure ticker is valid:
            prices = []
            try:
                prices = yfinance.download(ticker.upper(), start=datetime.now() - timedelta(days=1), end=datetime.now(), progress=False)
            except:
                pass
            if len(prices) == 0:
                log.info(f"Skipping {ticker} since ticker is invalid.")
            else:
                rendered = create_table_template.render(ticker=ticker)
                table_name = template_patt.search(rendered)[1]
                log.info(f"Creating table name {table_name}.")      
                cursor.execute(rendered)
                table_names[ticker] = table_name
    # Updated associated variable:
    log.info(f"Updating historical_stock_prices_tables variable with {len(table_names)} new tables.")
    if var_exists:
        Variable.update(key='historical_stock_prices_tables', value=json.dumps(table_names))
    else:
        Variable.set(key='historical_stock_prices_tables', value=json.dumps(table_names))

def get_and_insert_prices(**context):
    """
    * Get historical prices from yfinance and insert.
    """
    log = context['log']
    tickers = [ticker.upper() for ticker in context["tickers"].split(",")]
    start_date = dtparser(context["start_date"])
    end_date = dtparser(context["end_date"])
    if start_date > end_date:
        cp = start_date
        start_date = end_date
        end_date = cp
    historical_stock_prices_tables = Variable.set(key='historical_stock_prices_tables', default={})
    if not historical_stock_prices_tables:
        raise AirflowException("No historical stock prices tables have been generated. Ending DAG.")
    pg_hook = PostgresHook(conn_id=context["conn_id"])
    with pg_hook.get_conn() as conn:    
        for ticker in tickers:
            if not ticker in historical_stock_prices_tables:
                log.info(f"Skipping {ticker} since no table generated.")
                continue
            schema, table = historical_stock_prices_tables[ticker].split(".")
            results = yfinance.download(ticker.upper(), start=context["start_date"], end=context["end_date"], progress=False)
            log.info(f"Inserting {len(results)} into {schema}.{table}.")
            results.to_sql(name=table, schema=schema, con=conn, if_exists="append", index=False)
    
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=datetime.now(),
    schedule=None,
    params = {"tickers" : Param("ticker", type="string", description="Ticker to pull prices."),
              "start_date" : Param("start_date", type="string", description="Start date for data pull."),
              "end_date" : Param("end_date", type="string", description="End date for data pull.")},
    render_template_as_native_obj=True
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    op_kwargs = {"log" : log}
    op_kwargs["conn_id"] = "postgres_default"
    op_kwargs["tickers"] = "{{ params.tickers }}"
    op_kwargs["start_date"] = "{{ params.start_date }}"
    op_kwargs["end_date"] = "{{ params.end_date }}"
    op_kwargs["required"] = {"tickers" : (str, ), "start_date" : (str, is_datetime), "end_date" : (str, is_datetime)}
    
    start_task = EmptyOperator(task_id='start', dag=dag)
    
    check_params_task = PythonOperator(task_id='check_params',
                                        python_callable=check_params,
                                        op_kwargs=op_kwargs)
    generate_tables_task = PythonOperator(task_id="generate_tables",
                                          python_callable=generate_tables,
                                          op_kwargs=op_kwargs)
    get_and_insert_prices_task = PythonOperator(task_id='get_and_insert_prices',
                                        python_callable=get_and_insert_prices,
                                        op_kwargs=op_kwargs)
    
    
    start_task >> check_params_task >> generate_tables_task >> get_and_insert_prices_task
    


