from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import get_dag_name, get_tickers, get_variable_values
import os
import logging
import pandas
import yahoo_fin.stock_info as si
    
def get_and_insert_earnings_dates(**context):
    """ Get future earnings dates over next six months
    and insert into table.
    """
    log = context['log']
    log.info('Starting get_earnings_dates()')
    variable_values = context['ti'].xcom_pull(task_ids='get_variables', key='variable_values')
    tickers_to_track = context['ti'].xcom_pull(task_ids='get_tickers', key='tickers_to_track')
    pg_hook = PostgresHook(conn_id=context['conn_id'])
    pg_conn = pg_hook.get_connect()
    tickers_to_track_table = variable_values['tickers_to_track_table']
    log.info('Getting earnings dates for companies loaded in %s table', tickers_to_track_table)
    data = {'company_id' : [], 'earnings_date' : []}
    for company_id, ticker in tickers_to_track:
        earnings_date = si.get_next_earnings_date(ticker)
        data['company_id'].append(company_id)
        data['earnings_date'].append(earnings_date)
    # Insert into target table:
    data = pandas.DataFrame(data)
    data.to_sql('company_earnings_calendar', con=pg_conn, if_exists='append', index=False)

with DAG(
    dag_id=get_dag_name(__file__),
    catchup=False,
    schedule="@once",
    start_date=days_ago(-1)
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    
    start = EmptyOperator(task_id='start')
    
    get_variables_task = PythonOperator(task_id='get_variables',
                                        python_callable=get_variable_values, 
                                        provide_context=True,
                                        op_kwargs={'variable_list' : ['tickers_to_track_table', 'company_earnings_calendar_table']})

    
    get_tickers_task = PythonOperator(task_id='get_tickers',
                                      python_callable=get_tickers,
                                      provide_context=True,
                                      op_kwargs={'log':log, 'conn_id' : 'postgres_default'})
    
    get_earnings_dates_task = PythonOperator(task_id='get_earnings_dates',
                                             python_callable=get_and_insert_earnings_dates,
                                             provide_context=True,
                                             op_kwargs={'log': log, 'conn_id':'postgres_default'})
    
    
    start >> get_variables_task >> get_tickers_task >> get_earnings_dates_task

    