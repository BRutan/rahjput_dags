from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from common.helpers import get_tickers, get_columns_to_write, get_variable_values, connect_postgres
from common import *
from datetime import datetime
from jinja2 import Template
import logging
import pandas as pd
import yfinance
import sched, time

##############
# Steps:
##############
def get_and_insert_option_chains(**context)-> None:
    """Get option chain for single ticker and insert into postgres database.
    Perform every interval specified in "option_chain_pull_interval_minutes" variable. 
    """
    log = context['log']
    ticker = context['ticker']
    log.info(f'Starting get_and_insert_option_chains for ticker {ticker}.')
    scheduler = context['scheduler']
    end_time = context['end_time']
    postgres_conn = context['ti'].xcom_pull(task_ids='connect_postgres', key='postgres_conn')
    variable_values = context['ti'].xcom_pull(task_ids='get_variables', key='variable_values')
    columns_to_write = context['ti'].xcom_pull(task_ids='get_columns_to_write', key='columns_to_write')
    columns_to_write = list(set(columns_to_write) - set(['expirationDate', 'isCall']))
    interval = variable_values['option_chain_pull_interval_minutes']
    target_table = variable_values['option_chain_template']
    template = Template(source=target_table)
    target_table = template.render(ticker=ticker)
    tk = yfinance.Ticker(ticker)  
    log.info(f'Pulling data for {ticker} every {interval} minutes. Ending at {str(end_time)}.')
    params = (interval, tk, ticker, postgres_conn, target_table, columns_to_write, scheduler, log, end_time)
    scheduler.enter(interval, 1, get_option_chain_and_insert, params)
    scheduler.run()
        
###########
# Helpers:
###########
def get_option_chain_and_insert(interval, tk, ticker, postgres_conn, target_table, columns_to_write, scheduler, log, end_time):
    now = datetime.datetime.now()
    if now > end_time:
        return 
    log.info(f'Pulling options chains for {ticker} at {str(now)}.')
    exps = tk.options
    data = {col : [] for col in columns_to_write}
    data['expirationDate'] = []
    data['isCall'] = []
    for exp in exps:
        opt = tk.option_chain(exp)
        calls = opt.calls
        for col in columns_to_write:
            data[col].extend(calls[col])
        data['isCall'].extend([True] * len(calls))
        puts = opt.puts
        for col in columns_to_write:
            data[col].extend(puts[col])
        data['isCall'].extend([False] * len(calls))
        data['expirationDate'].extend([exp] * (len(calls) + len(puts)))
    # Insert:
    data = pd.DataFrame(data)
    data.to_sql(target_table, con=postgres_conn, if_exists='append')
    scheduler.enter(interval, 1, get_option_chain_and_insert, (interval, tk, ticker, postgres_conn, target_table, columns_to_write, scheduler, log, end_time))
        
###########
# Dag:
###########
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(-1),
    schedule="30 9 * * 1-5"
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    scheduler = sched.scheduler(time.time, time.sleep)
    present = datetime.now()
    end_time = datetime(year=present.year, month=present.month, day=present.day, hour=16, minute=0)
    
    start = EmptyOperator(task_id = 'start')
    
    get_variables_task = PythonOperator(task_id='get_variables',
                                    python_callable=get_variable_values, 
                                    provide_context=True,
                                    op_kwargs={log:log,
                                               'variable_list' : ['tickers_to_track_table', 'option_chain_template','option_chain_pull_interval_minutes']})
    
    connect_postgres_task = PythonOperator(task_id='connect_postgres',
                                        python_callable=connect_postgres,
                                        provide_context=True,
                                        op_kwargs={'conn_id':'postgres_default','log':log})
    
    get_tickers_task = PythonOperator(task_id='get_tickers',
                                      python_callable=get_tickers,
                                      provide_context=True,
                                      op_kwargs={'log' : log})
    
    tickers = "{{ ti.xcom_pull(task_ids='get_tickers', key='tickers_to_track') }}"
    
    get_columns_to_write_task = PythonOperator(task_id='get_columns_to_write',
                                            python_callable=get_columns_to_write,
                                            provide_context=True,
                                            op_kwargs={'log': log, 
                                                        'table_name' : '{tickers[0]}_option_chains', 
                                                        'schema_name' : 'data'})
    
    get_and_insert_option_chains_tasks = []
    for ticker in tickers:
        get_and_insert_option_chains_tasks.append(PythonOperator(task_id='get_and_insert_option_chains',
                                                        python_callable=get_and_insert_option_chains,
                                                        provide_context=True,
                                                        op_kwargs={'log':log, 
                                                                   'scheduler' : scheduler,
                                                                   'ticker':ticker, 
                                                                   'end_time' : end_time}))
    
    start >> get_variables_task >> connect_postgres_task >> get_tickers_task >> get_columns_to_write_task >> get_and_insert_option_chains_tasks
    
    
    
    

    