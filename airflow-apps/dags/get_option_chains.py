from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_columns_to_write, get_dag_name, get_variable_values
from datetime import datetime
import logging
import os
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
    variable_values = context['ti'].xcom_pull(task_ids='get_variables', key='variable_values')
    columns_to_write = context['ti'].xcom_pull(task_ids='get_columns_to_write', key='columns_to_write')
    log = context['log']
    ticker = context['ticker']
    target_table = context['target_table']
    target_schema = context.get('target_schema', None)
    scheduler = context['scheduler']
    end_time = context['end_time']
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    with pg_hook.get_conn() as pg_conn:
        columns_to_write = set(columns_to_write) - set(['expirationdate', 'iscall'])
        interval = int(variable_values['option_chain_pull_interval_minutes'])
        tk = yfinance.Ticker(ticker)  
        log.info(f'Pulling data for {ticker} every {interval} minutes. Ending at {str(end_time)}.')
        params = (interval, tk, ticker, pg_conn, target_table, target_schema, columns_to_write, scheduler, log, end_time)
        scheduler.enter(interval, 1, get_option_chain_and_insert, params)
        scheduler.run()
        
###########
# Helpers:
###########
def get_option_chain_and_insert(interval, tk, ticker, pg_conn, target_table, target_schema, columns_to_write, scheduler, log, end_time):
    now = datetime.now()
    if now > end_time:
        return 
    log.info(f'Pulling options chains for {ticker} at {str(now)}.')
    exps = tk.options
    data = {col : [] for col in columns_to_write}
    data['expirationdate'] = []
    data['iscall'] = []
    for exp in exps:
        opt = tk.option_chain(exp)
        calls = opt.calls
        for col in calls.columns:
            if col.lower() in data:
                data[col.lower()].extend(calls[col])
        data['iscall'].extend([True] * len(calls))
        data['expirationdate'].extend([exp] * len(calls))
        puts = opt.puts
        for col in opt.puts:
            if col.lower() in data:
                data[col.lower()].extend(puts[col])
        data['iscall'].extend([False] * len(calls))
        data['expirationdate'].extend([exp] * len(puts))
    # Insert:
    if len(data['expirationdate']) == 0:
        log.warn(f'No option chains available for {ticker}.')
    else:
        log.info(f'Inserting into {target_table}.')
        log.info([f'{col}:{len(data[col])}' for col in data])
        data = pd.DataFrame(data)
        data.to_sql(name=target_table, schema=target_schema, con=pg_conn, if_exists='append')
    scheduler.enter(interval, 1, get_option_chain_and_insert, (interval, tk, ticker, pg_conn, target_table, target_schema, columns_to_write, scheduler, log, end_time))
        
###########
# Dag:
###########
with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    start_date=days_ago(1),
    schedule="@once"#"30 9 * * 1-5"
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    scheduler = sched.scheduler(time.time, time.sleep)
    present = datetime.now()
    end_time = datetime(year=present.year, month=present.month, day=present.day + 1, hour=1, minute=0)
    
    start = EmptyOperator(task_id = 'start')
    
    option_chain_tables = Variable.get('option_chains_tables', [], deserialize_json=True)
    if len(option_chain_tables) > 1:
        tickers = list(option_chain_tables.keys())
        ex_ticker = tickers[0]
        ex_table = option_chain_tables[ex_ticker]
        
        get_variables_task = PythonOperator(task_id='get_variables',
                                        python_callable=get_variable_values, 
                                        provide_context=True,
                                        op_kwargs={'log':log,
                                                   'variables_list' : ['option_chain_pull_interval_minutes']},
                                        dag=dag)
        
        get_columns_to_write_task = PythonOperator(task_id='get_columns_to_write',
                                                python_callable=get_columns_to_write,
                                                provide_context=True,
                                                op_kwargs={'log': log, 
                                                            'table_name' : ex_table,
                                                            'conn_id': 'postgres_default'},
                                                dag=dag)
        
        get_and_insert_option_chains_tasks = []
        for ticker in option_chain_tables:
            operator = PythonOperator(task_id=f'get_and_insert_option_chains_{ticker}',
                                    python_callable=get_and_insert_option_chains,
                                    provide_context=True,
                                    op_kwargs={'log':log, 
                                                'scheduler' : scheduler,
                                                'ticker':ticker, 
                                                'end_time' : end_time,
                                                'target_table': option_chain_tables[ticker], 
                                                'conn_id': 'postgres_default'},
                                        dag=dag)
            get_and_insert_option_chains_tasks.append(operator)
    
        start >> get_variables_task >>  get_columns_to_write_task >> get_and_insert_option_chains_tasks
    else:
        start >> EmptyOperator(task_id='No_tickers_present.')
    
    
    
    

    