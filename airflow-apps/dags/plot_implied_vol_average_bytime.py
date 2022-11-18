from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from common import is_datetime, get_dag_name
from datetime import datetime, timedelta
from dateutil.parser import parse as dtparse
import logging
import matplotlib.pyplot as plt
import os

def check_params(**context):
    errs = []
    for elem in ["start_date", "end_date"]:
        if not is_datetime(context[elem]):
            errs.append(f"{elem} is not valid datetime {context[elem]}.")
    if not context["freq"] in ["d", "w", "m", "y"]:
        errs.append("freq must be one of ['d','w','m','y'].")
    tickers= Variable.get("option_chains_tables", {})
    if not context["ticker"].upper() in tickers:
        errs.append(f"Options chain table not created for {context['ticker']}.")
    if not os.path.exists(context["output_dir"]):
        errs.append(f"output_dir {context['output_dir']} does not exist.")
    if errs:
        raise ValueError("\n".join(errs))

def get_and_plot_implied_vol(**context):
    """
    * Plot implied vol using options chains.
    """
    log = context["log"]
    ticker = context["ticker"].lower()
    tickers = Variable.get("option_chains_tables", {})
    option_chain_table = tickers[ticker]
    log.info("Starting get_and_plot_implied_vol().")
    # Check that parameters valid:
    pg_hook = PostgresHook(conn_id=context["conn_id"])
    log.info(f"Pulling data for {ticker} starting on ")
    with pg_hook.get_conn() as connection:
        cursor = connection.cursor()
        query = [",AVG(implied_volatility)"]
        query.append(f"FROM {option_chain_table}")
        query.append(f"WHERE upload_timestamp >= {context['start_date']} AND upload_timestamp <= {context['end_date']}")
        if context["freq"] == "d":
            query.insert(0, "SELECT CONCAT(MONTH(upload_timestamp), '/',DAY(upload_timestamp),'/',YEAR(upload_timestamp))")
            query.append("GROUP BY MONTH(upload_timestamp), DAY(upload_timestamp), YEAR(upload_timestamp)")
        elif context["freq"] == "m":
            query.insert(0, "SELECT CONCAT(MONTH(upload_timestamp), '/',YEAR(upload_timestamp))")
            query.append("GROUP BY MONTH(upload_timestamp), YEAR(upload_timestamp)")
        elif context["freq"] == "w":
            # ToDo: Implement weekly:
            query.insert(0, "SELECT CONCAT(MONTH(upload_timestamp), '/',DAY(upload_timestamp),'/',YEAR(upload_timestamp))")
            query.append("GROUP BY MONTH(upload_timestamp), YEAR(upload_timestamp)")
        else:
            query.insert(0, "SELECT YEAR(upload_timestamp), AVG(implied_volatility)")
            query.append("GROUP BY YEAR(upload_timestamp), date_trunc('week', upload_timestamp)")
        cursor.execute(" ".join(query))
        data = cursor.fetchall()
        if not len(data) > 0:
            log.info("No data found with parameters. Skipping graph generation.")
            return
        else:
            context["data"] = data
            generate_graph(context)
    log.info("Ending get_and_plot_implied_vol().")
    
def generate_graph(context):
    """
    * Generate graph using matplotlib.
    """
    log = context["log"]
    log.info("Starting generate_graph().")
    data = context["data"]
    outpath = os.path.join(context["output_dir"])
    plt.plot()
    log.info(f"Saving figure to {outpath}.")
    plt.savefig(context["output_dir"])
    
    log.info("Ending generate_graph().")
    

with DAG(
    dag_id=get_dag_name(__file__),
    start_date=datetime.now(),
    catchup=False,
    schedule="@once",
    params= {"ticker" : Param("", type="string"),
             "start_date" : Param("", type="string"),
             "end_date" : Param("", type="string"),
             "output_folder" : Param("", type="string"),
             # Optional:
             "freq" : Param(type="string", default="d", description="Data frequency. Must be in ['d','w','m','y']."),
             "strike" : Param(type="string", default="")}
) as dag:
    
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    
    start = EmptyOperator(task_id="start", 
                          dag=dag)
    op_kwargs = dag.params
    op_kwargs["log"] = log
    op_kwargs["conn_id"] = "postgres_default"
    check_params_task = PythonOperator(task_id="check_params",
                                       op_kwargs=op_kwargs,
                                       python_callable=check_params,
                                       dag=dag)
    
    get_and_plot_implied_vol_task = PythonOperator(task_id="get_and_plot_implied_vol",
                                                   op_kwargs=op_kwargs,
                                                   python_callable=get_and_plot_implied_vol,
                                                   dag=dag)
    
    start >> check_params_task >> get_and_plot_implied_vol_task